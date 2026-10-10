require "uri"

class LavinMQCtl
  class TUI
    enum ViewKind
      Queue
      Connection
      Channel
      # The fields of a table row, for what has no view of its own
      Fields
    end

    # A list in a view, like a queue's consumers. Its rows are the object's
    # *key* field, or fetched from the object's path followed by *path*,
    # sorted by *sort*, descending. Without columns it's the object's fields.
    # *count* is the object's field with the number of rows, and Enter on a
    # row opens the view *open* returns for it.
    record Section, title : String, columns : Array(Column) = [] of Column,
      key : String? = nil, path : String? = nil, sort : String? = nil, count : String? = nil,
      open : Proc(JSON::Any, View?)? = nil do
      def fields? : Bool
        columns.empty?
      end
    end

    # An object opened with Enter, from a table or from a row in another view
    class View
      getter kind : ViewKind
      getter name : String
      # The object's API path, empty for a table row's fields
      getter path : String
      # The table row it was opened from, if any, until it's fetched
      property item : JSON::Any
      # For a table row's fields, to find the row again after a refresh
      getter row_id : String
      property section = 0
      property cursor = 0
      property scroll = 0
      property rows = [] of JSON::Any
      # The row number of the first of rows
      property rows_first = 0
      property rows_total = 0
      # Not found when last fetched, so what's shown is its last state
      property? gone = false
      # Message counts seen while it's open, for brokers without their logs
      getter ready_history = [] of Float64
      getter unacked_history = [] of Float64
      getter opened_at = Time.instant

      def initialize(@kind : ViewKind, @name : String, @path : String, @item : JSON::Any, @row_id = "")
      end
    end

    @sections : Hash(ViewKind, Array(Section))

    EMPTY_ITEM = JSON::Any.new({} of String => JSON::Any)

    # How many views can be opened on top of each other, the oldest is
    # forgotten after that
    MAX_VIEWS = 32

    GRAPHS_HEIGHT = 10
    # Width of a label and its value in a view's summary, and of the label
    SUMMARY_PAIR_WIDTH  = 30
    SUMMARY_LABEL_WIDTH = 11

    private def view_sections : Hash(ViewKind, Array(Section))
      fields = Section.new("Fields")
      {
        ViewKind::Queue => [
          Section.new("Consumers", [
            col("Tag", 28, flex: true) { |c| Fields.text(c, "consumer_tag") },
            col("Channel", 30) { |c| Fields.text(c, "channel_details", "name") },
            col("Ack", 4) { |c| Fields.bool(c, "ack_required") },
            col("Exclusive", 9) { |c| Fields.bool(c, "exclusive") },
            col("Prefetch", 8, right: true) { |c| Fields.count(c, "prefetch_count") },
            col("Priority", 8, right: true) { |c| Fields.count(c, "priority") },
          ], key: "consumer_details", count: "consumers",
            open: ->(c : JSON::Any) { channel_view(Fields.text(c, "channel_details", "name", default: "")) }),
          Section.new("Bindings", [
            col("Exchange", 30, flex: true) { |b| Fields.text(b, "source").presence || "(default)" },
            col("Routing key", 30) { |b| Fields.text(b, "routing_key") },
            col("Arguments", 40) { |b| Fields.text(b, "arguments") },
          ], path: "/bindings"),
          Section.new("Unacked", [
            col("Delivery tag", 12, right: true) { |u| Fields.count(u, "delivery_tag") },
            col("Unacked for", 11, right: true) { |u| Fields.seconds(u, "unacked_for_seconds") },
            col("Consumer", 28) { |u| Fields.text(u, "consumer_tag") },
            col("Channel", 30, flex: true) { |u| Fields.text(u, "channel_name") },
          ], path: "/unacked", sort: "unacked_for_seconds", count: "messages_unacknowledged",
            open: ->(u : JSON::Any) { channel_view(Fields.text(u, "channel_name", default: "")) }),
          fields,
        ],
        ViewKind::Connection => [
          Section.new("Channels", [
            col("Channel", 30, flex: true) { |c| Fields.text(c, "name") },
            col("State", 10, status: true) { |c| Fields.text(c, "state") },
            col("Unacked", 10, right: true, color: prefetch_color) { |c| Fields.count(c, "messages_unacknowledged") },
            col("Prefetch", 8, right: true) { |c| Fields.count(c, "prefetch_count") },
            col("Cons", 6, right: true) { |c| Fields.count(c, "consumer_count") },
            col("Pub/s", 11, right: true) { |c| Fields.rate(c, "message_stats", "publish_details", "rate") },
            col("Confirm", 7) { |c| Fields.bool(c, "confirm") },
          ], path: "/channels", count: "channels",
            open: ->(c : JSON::Any) { channel_view(Fields.text(c, "name", default: "")) }),
          fields,
        ],
        ViewKind::Channel => [
          Section.new("Consumers", [
            col("Tag", 28, flex: true) { |c| Fields.text(c, "consumer_tag") },
            col("Queue", 30) { |c| Fields.text(c, "queue", "name") },
            col("Ack", 4) { |c| Fields.bool(c, "ack_required") },
            col("Exclusive", 9) { |c| Fields.bool(c, "exclusive") },
            col("Prefetch", 8, right: true) { |c| Fields.count(c, "prefetch_count") },
          ], key: "consumer_details", count: "consumer_count",
            open: ->(c : JSON::Any) { queue_view(Fields.text(c, "queue", "vhost", default: ""), Fields.text(c, "queue", "name", default: "")) }),
          fields,
        ],
        ViewKind::Fields => [fields],
      }
    end

    private def queue_view(vhost : String, name : String, item = EMPTY_ITEM) : View?
      return if name.empty?
      View.new(ViewKind::Queue, name, "/api/queues/#{URI.encode_path_segment(vhost)}/#{URI.encode_path_segment(name)}", item)
    end

    private def connection_view(name : String, item = EMPTY_ITEM) : View?
      return if name.empty?
      View.new(ViewKind::Connection, name, "/api/connections/#{URI.encode_path_segment(name)}", item)
    end

    private def channel_view(name : String, item = EMPTY_ITEM) : View?
      return if name.empty?
      View.new(ViewKind::Channel, name, "/api/channels/#{URI.encode_path_segment(name)}", item)
    end

    # The view of the selected row: a queue, connection or channel, a
    # consumer's channel, or else the row's fields
    private def open_view
      return unless state = table_state
      index = state.cursor - @items_first
      return unless index >= 0 && (item = @items[index]?)
      view = case @page
             when :queues      then queue_view(Fields.text(item, "vhost", default: ""), Fields.text(item, "name", default: ""), item)
             when :connections then connection_view(Fields.text(item, "name", default: ""), item)
             when :channels    then channel_view(Fields.text(item, "name", default: ""), item)
             when :consumers   then channel_view(Fields.text(item, "channel_details", "name", default: ""))
             end
      push_view(state, view || View.new(ViewKind::Fields, row_name(item), "", item, item_id(item)))
    end

    private def push_view(state : TableState, view : View)
      state.views.shift if state.views.size >= MAX_VIEWS
      state.views << view
    end

    private def row_name(item : JSON::Any) : String
      name = Fields.text(item, "name", default: Fields.text(item, "consumer_tag", default: Fields.text(item, "upstream")))
      name.presence || "(default)"
    end

    private def section(view : View) : Section
      @sections[view.kind][view.section]
    end

    # Esc goes back, Tab and Left and Right switch sections and Enter opens
    # the selected row. Keys for the table under it do nothing, others work
    # as anywhere.
    private def view_key(state : TableState, view : View, event : KeyEvent) : Bool
      key = event.key.char? ? VI_KEYS[event.char]? : event.key
      return event.char.in?('o', 'r', '/') unless key
      case key
      when .escape?, .backspace? then state.views.pop
      when .tab?, .right?        then switch_section(view, 1)
      when .back_tab?, .left?    then switch_section(view, -1)
      when .enter?               then open_row(state, view)
      else                            move_in_view(view, move_delta(view, key))
      end
      true
    end

    private def move_delta(view : View, key : Key) : Int32
      case key
      when .up?        then -1
      when .down?      then 1
      when .page_up?   then -section_rows(view)
      when .page_down? then section_rows(view)
      when .home?      then Int32::MIN
      when .end?       then Int32::MAX
      else                  0
      end
    end

    private def switch_section(view : View, delta : Int32)
      view.section = (view.section + delta) % @sections[view.kind].size
      view.cursor = 0
      view.scroll = 0
      view.rows = [] of JSON::Any
      view.rows_first = 0
      view.rows_total = 0
    end

    # Fields are scrolled, other sections move the selection. The scroll is
    # limited when drawn, as it depends on the terminal size.
    private def move_in_view(view : View, delta : Int32)
      if section(view).fields?
        view.scroll = (view.scroll.to_i64 + delta).clamp(0, Int32::MAX).to_i32
      else
        view.cursor = (view.cursor.to_i64 + delta).clamp(0, {view.rows_total - 1, 0}.max).to_i32
      end
    end

    private def open_row(state : TableState, view : View)
      return unless open = section(view).open
      index = view.cursor - view.rows_first
      return unless index >= 0 && (row = view.rows[index]?)
      if child = open.call(row)
        push_view(state, child)
      end
    end

    private def view_fetch(view : View) : String
      rows = section_rows(view)
      "view #{view.kind} #{view.path}#{view.row_id} #{view.section} #{view.cursor // rows} #{rows}"
    end

    # The view's object and the rows of its section. A table row's fields
    # are found in the table again.
    private def refresh_view(view : View)
      if view.kind.fields?
        fetch_table
        if item = @items.find { |i| item_id(i) == view.row_id }
          view.item = item
          view.gone = false
        else
          view.gone = true
        end
        return
      end

      section = section(view)
      rows = section_rows(view)
      path = view.path
      if view.kind.queue?
        # The consumers up to the end of the page shown, the API can't skip any
        consumers = section.key ? (view.cursor // rows + 1) * rows : 1
        path += "?consumer_list_length=#{consumers}"
      end
      if item = fetch_json(path, view.kind.to_s.downcase, missing: true)
        if item.raw.nil?
          view.gone = true
        else
          view.item = item
          view.gone = false
          record_counts(view)
        end
      end
      fetch_section(view, section, rows) unless view.gone?
    end

    private def fetch_section(view : View, section : Section, rows : Int32)
      if key = section.key
        all = Fields.dig(view.item, key).try(&.as_a?) || [] of JSON::Any
        count = section.count.try { |c| Fields.int(view.item, c) } || 0_i64
        view.rows_total = {count.clamp(0, Int32::MAX).to_i32, all.size}.max
        view.cursor = view.cursor.clamp(0, {view.rows_total - 1, 0}.max)
        view.rows_first = view.cursor // rows * rows
        view.rows = all[view.rows_first, rows]? || [] of JSON::Any
      elsif path = section.path
        # Once more if the rows shrank and the cursor ended up past the end
        2.times do
          page = view.cursor // rows
          view.rows_first = page * rows
          view.rows, view.rows_total = fetch_page(view.path + path, section.title.downcase, page + 1, rows, section.sort, true, "")
          last = {view.rows_total - 1, 0}.max
          break if view.cursor <= last
          view.cursor = last
        end
      end
    end

    # For brokers that don't send a queue's message count logs
    private def record_counts(view : View)
      return unless view.kind.queue?
      {view.ready_history => "messages_ready", view.unacked_history => "messages_unacknowledged"}.each do |history, key|
        history << Fields.float(view.item, key)
        history.shift if history.size > 240
      end
    end

    # Its summary at the top, graphs below it when there's room, and the
    # sections in what's left
    private def view_layout(view : View) : {Rect, Rect?, Rect}
      width = @width - 2
      bottom = @height - 3
      # A line for a warning, so the layout doesn't change when one shows up,
      # and only that in a small terminal
      pair_lines = summary_pair_lines(view, width)
      summary_height = pair_lines.zero? ? 3 : pair_lines + 4
      summary = Rect.new(1, 2, width, summary_height)
      y = summary.bottom + 1
      graphs = nil
      if bottom - y + 1 >= GRAPHS_HEIGHT + 12
        graphs = Rect.new(1, y, width, GRAPHS_HEIGHT)
        y += GRAPHS_HEIGHT
      end
      {summary, graphs, Rect.new(1, y, width, {bottom - y + 1, 0}.max)}
    end

    # Rows of the section that fit below its header
    private def section_rows(view : View) : Int32
      {view_layout(view)[2].height - 4, 1}.max
    end

    private def summary_pair_lines(view : View, width : Int32) : Int32
      per_line = {(width - 6) // SUMMARY_PAIR_WIDTH, 1}.max
      needed = (summary_pairs(view).size + per_line - 1) // per_line
      # Leaves room for the section's header and a few of its rows
      {needed, {@height - 4 - 4 - 8, 0}.max}.min
    end

    private def draw_view(state : TableState, view : View)
      title = breadcrumb(state)
      if view.kind.fields?
        note = view.gone? ? "not on this page anymore" : ""
        return draw_fields(Rect.new(1, 2, @width - 2, @height - 4), view, title, note)
      end
      summary, graphs, section = view_layout(view)
      draw_summary(summary, view, title)
      draw_view_graphs(graphs, view) if graphs
      draw_section(section, view)
    end

    # The table's title and the names of the views opened, the first ones
    # left out if they don't fit
    private def breadcrumb(state : TableState) : String
      parts = [@tables[@page].title] + state.views.map(&.name)
      max = @width - 10
      while parts.size > 2 && Text.width(parts.join(" › ")) > max
        parts.delete_at(1)
        parts[1] = "…" unless parts[1] == "…"
      end
      parts.join(" › ")
    end

    private def draw_summary(rect : Rect, view : View, title : String)
      draw_panel(rect, title)
      pairs = summary_pairs(view)
      per_line = {(rect.width - 6) // SUMMARY_PAIR_WIDTH, 1}.max
      lines = summary_pair_lines(view, rect.width)
      pairs.first(lines * per_line).each_with_index do |(label, value, color), i|
        x = rect.inner_x + 2 + (i % per_line) * SUMMARY_PAIR_WIDTH
        y = rect.inner_y + 1 + i // per_line
        print_fit(x, y, label, SUMMARY_LABEL_WIDTH, MUTED_FG, PANEL_BG)
        x += SUMMARY_LABEL_WIDTH
        value_width = SUMMARY_PAIR_WIDTH - SUMMARY_LABEL_WIDTH - 1
        if label == "State" && value != "-"
          print_at(x, y, "● ", state_color(value), PANEL_BG)
          print_fit(x + 2, y, value, value_width - 2, color, PANEL_BG, true)
        else
          print_fit(x, y, value, value_width, color, PANEL_BG, true)
        end
      end
      if warning = view_warning(view)
        text, color = warning
        y = lines.zero? ? rect.inner_y : rect.inner_y + 1 + lines
        print_at(rect.inner_x + 2, y, "▲ #{text}", color, PANEL_BG, true, rect.inner_width - 4)
      end
    end

    # Labels and values at the top of a view
    private def summary_pairs(view : View) : Array({String, String, Color})
      item = view.item
      case view.kind
      when .queue?
        ready_color = queue_waiting?(item) ? YELLOW : WHITE
        [
          {"State", Fields.text(item, "state"), WHITE},
          {"Type", Fields.text(item, "arguments", "x-queue-type", default: "classic"), WHITE},
          {"Vhost", Fields.text(item, "vhost"), WHITE},
          {"Policy", Fields.text(item, "policy"), WHITE},
          {"Ready", Fields.count(item, "messages_ready"), ready_color},
          {"Unacked", Fields.count(item, "messages_unacknowledged"), WHITE},
          {"Consumers", Fields.count(item, "consumers"), WHITE},
          {"Size", Fields.dig(item, "total_bytes") ? Fields.bytes(item, "total_bytes") : "-", WHITE},
          {"Publish", "#{Fields.rate(item, "message_stats", "publish_details", "rate")}/s", GREEN},
          {"Deliver", "#{Fields.rate(item, "message_stats", "deliver_get_details", "rate")}/s", BLUE},
          {"Durable", Fields.bool(item, "durable"), WHITE},
          {"Exclusive", Fields.bool(item, "exclusive"), WHITE},
        ]
      when .connection?
        client = Fields.text(item, "client_properties", "connection_name", default: Fields.text(item, "client_properties", "product"))
        tls = Fields.dig(item, "ssl").try(&.as_bool?) ? Fields.text(item, "tls_version", default: "yes") : "no"
        [
          {"State", Fields.text(item, "state"), WHITE},
          {"User", Fields.text(item, "user"), WHITE},
          {"Vhost", Fields.text(item, "vhost"), WHITE},
          {"Client", client, WHITE},
          {"Channels", Fields.count(item, "channels"), WHITE},
          {"Protocol", Fields.text(item, "protocol"), WHITE},
          {"TLS", tls, WHITE},
          {"Heartbeat", heartbeat(item), WHITE},
          {"Recv", Fields.bytes_rate(item, "recv_oct_details", "rate"), GREEN},
          {"Send", Fields.bytes_rate(item, "send_oct_details", "rate"), BLUE},
          {"Connected", connected_for(item), WHITE},
        ]
      when .channel?
        [
          {"State", Fields.text(item, "state"), WHITE},
          {"User", Fields.text(item, "user"), WHITE},
          {"Vhost", Fields.text(item, "vhost"), WHITE},
          {"Consumers", Fields.count(item, "consumer_count"), WHITE},
          {"Unacked", Fields.count(item, "messages_unacknowledged"), prefetch_full?(item) ? YELLOW : WHITE},
          {"Prefetch", Fields.count(item, "prefetch_count"), WHITE},
          {"Global pf", Fields.count(item, "global_prefetch_count"), WHITE},
          {"Confirm", Fields.bool(item, "confirm"), WHITE},
          {"Publish", "#{Fields.rate(item, "message_stats", "publish_details", "rate")}/s", GREEN},
          {"Deliver", "#{Fields.rate(item, "message_stats", "deliver_get_details", "rate")}/s", BLUE},
          {"Connection", Fields.text(item, "connection_details", "name"), WHITE},
        ]
      else
        [] of {String, String, Color}
      end
    end

    private def heartbeat(item : JSON::Any) : String
      return "-" unless Fields.dig(item, "timeout")
      timeout = Fields.int(item, "timeout")
      timeout > 0 ? "#{timeout}s" : "off"
    end

    private def connected_for(item : JSON::Any) : String
      connected_at = Fields.int(item, "connected_at")
      return "-" unless connected_at > 0
      ago = Time.utc.to_unix_ms - connected_at
      ago >= 0 ? "#{Fields.seconds_text(ago // 1000)} ago" : "-"
    end

    # Messages are waiting, but nothing consumes them
    private def queue_waiting?(item : JSON::Any?) : Bool
      Fields.int(item, "messages_ready") > 0 && Fields.int(item, "consumers") == 0
    end

    # Its consumers get no more messages until they ack some
    private def prefetch_full?(item : JSON::Any?) : Bool
      unacked = Fields.int(item, "messages_unacknowledged")
      global = Fields.int(item, "global_prefetch_count")
      limit = global > 0 ? global : Fields.int(item, "prefetch_count") * Fields.int(item, "consumer_count")
      limit > 0 && unacked >= limit
    end

    # What's worth knowing about the object when debugging
    private def view_warning(view : View) : {String, Color}?
      return {"Not found anymore, this is how it last was", RED} if view.gone?
      item = view.item
      case view.kind
      when .queue?
        case Fields.text(item, "state")
        when "paused" then {"Paused: consumers get no messages until it's resumed", YELLOW}
        when "closed" then {"Closed after an error, see the broker's log", RED}
        else
          {"No consumers: #{Fields.count(item, "messages_ready")} messages are waiting", YELLOW} if queue_waiting?(item)
        end
      when .connection?
        state = Fields.text(item, "state")
        {"#{state.capitalize}: the broker doesn't read what it publishes, for flow control", YELLOW} if state.in?("blocked", "blocking")
      when .channel?
        {"At its prefetch limit: its consumers get no messages until they ack some", YELLOW} if prefetch_full?(item)
      end
    end

    private def draw_view_graphs(rect : Rect, view : View)
      item = view.item
      case view.kind
      when .queue?
        half = rect.width >= 80 ? rect.width // 2 : rect.width
        rates = Rect.new(rect.x, rect.y, half, rect.height)
        draw_graph_panel(rates, "Message rates", Fields.history(item, "message_stats", "publish_details"),
          Fields.history(item, "message_stats", "deliver_get_details"), {"Publish", "Deliver"}, stats_interval, RATE_FORMAT)
        return unless half < rect.width
        counts = Rect.new(rect.x + half + 1, rect.y, rect.width - half - 1, rect.height)
        ready = Fields.count_history(item, "messages_ready", "messages_ready_log")
        if ready.size > 1
          unacked = Fields.count_history(item, "messages_unacknowledged", "messages_unacknowledged_log")
          draw_graph_panel(counts, "Queued messages", ready, unacked, {"Ready", "Unacked"}, stats_interval, COUNT_FORMAT)
        else
          # Seen while the view is open, at the refresh interval
          seen = view.ready_history.size
          step = seen > 1 ? (Time.instant - view.opened_at) / (seen - 1) : stats_interval
          draw_graph_panel(counts, "Queued messages", view.ready_history, view.unacked_history, {"Ready", "Unacked"}, step, COUNT_FORMAT)
        end
      when .connection?
        draw_graph_panel(rect, "Network", Fields.history(item, "recv_oct_details"), Fields.history(item, "send_oct_details"),
          {"Recv", "Send"}, stats_interval, BYTES_RATE_FORMAT)
      when .channel?
        draw_graph_panel(rect, "Message rates", Fields.history(item, "message_stats", "publish_details"),
          Fields.history(item, "message_stats", "deliver_get_details"), {"Publish", "Deliver"}, stats_interval, RATE_FORMAT)
      end
    end

    # The section tabs on the panel's top border, with the number of rows
    # for those that have one
    private def draw_section(rect : Rect, view : View)
      return if rect.height < 4
      sections = @sections[view.kind]
      section = sections[view.section]
      draw_panel(rect, "")
      right = rect.right - 1
      x = rect.x + 2
      sections.each_with_index do |s, i|
        count = s.count.try { |c| Fields.dig(view.item, c) ? Fields.count(view.item, c) : nil }
        label = count ? " #{s.title} #{count} " : " #{s.title} "
        current = i == view.section
        x += print_at(x, rect.y, label, current ? WHITE : MUTED_FG, current ? SELECT_BG : PANEL_BG, current, right - x)
      end
      if section.fields?
        draw_field_lines(rect, view, x)
      else
        draw_section_rows(rect, view, section, x)
      end
    end

    private def draw_section_rows(rect : Rect, view : View, section : Section, note_x : Int32)
      if view.rows_total > view.rows.size
        note = "#{view.rows_first + 1}-#{view.rows_first + view.rows.size} of #{view.rows_total}"
        print_at(note_x + 1, rect.y, " #{note} ", MUTED_FG, PANEL_BG, max_width: rect.right - 1 - note_x - 1)
      end
      x = rect.inner_x + 2
      width = rect.inner_width - 4
      draw_row(rect.inner_y + 1, section.columns.map(&.title), section.columns, WHITE, PANEL_BG, true, x, width)
      if view.rows.empty?
        y = {rect.inner_y + 3, rect.bottom - 1}.min
        print_at(x, y, "None", MUTED_FG, PANEL_BG) if y > rect.inner_y + 1
        return
      end
      view.rows.each_with_index do |row, i|
        y = rect.inner_y + 2 + i
        break if y >= rect.bottom
        selected = view.rows_first + i == view.cursor
        bg = selected ? SELECT_BG : (i.even? ? PANEL_BG : ROW_BG)
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        set_cell(rect.inner_x + 1, y, '▌', GREEN, bg) if selected
        values = section.columns.map(&.value.call(row))
        draw_row(y, values, section.columns, selected ? WHITE : TEXT_FG, bg, selected, x, width, dots: true, item: row)
      end
    end

    # A table row's fields on their own, with *title* and *note* on the panel
    private def draw_fields(rect : Rect, view : View, title : String, note : String)
      draw_panel(rect, title)
      x = rect.x + 4 + Text.width(title)
      draw_field_lines(rect, view, x, note)
    end

    # Every field of the view's object, scrolled, with which lines are shown
    # and *note* on the border from *note_x*
    private def draw_field_lines(rect : Rect, view : View, note_x : Int32, note = "")
      fields = detail_fields(view.item)
      key_width = {fields.max_of? { |(key, _)| Text.width(key) } || 0, rect.inner_width // 3}.min
      value_width = rect.inner_width - key_width - 6
      lines = fields.flat_map do |(key, value)|
        wrap(value, value_width).map_with_index { |line, i| {i.zero? ? key : "", line} }
      end
      rows = {rect.inner_height - 2, 0}.max
      view.scroll = view.scroll.clamp(0, {lines.size - rows, 0}.max)
      text = String.build do |s|
        if lines.size > rows
          s << "lines " << view.scroll + 1 << "-" << {view.scroll + rows, lines.size}.min << " of " << lines.size
        end
        s << "  " << note unless note.empty?
      end
      unless text.strip.empty?
        print_at(note_x, rect.y, " #{text.lstrip} ", MUTED_FG, PANEL_BG, max_width: rect.right - 1 - note_x)
      end
      lines.skip(view.scroll).first(rows).each_with_index do |(key, value), i|
        y = rect.inner_y + 1 + i
        print_fit(rect.inner_x + 2, y, key, key_width, MUTED_FG, PANEL_BG)
        print_fit(rect.inner_x + 4 + key_width, y, value, value_width, WHITE, PANEL_BG)
      end
    end
  end
end
