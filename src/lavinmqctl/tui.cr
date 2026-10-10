require "json"
require "http/client"
require "uri"
require "./cli"
require "./tui/screen"
require "./tui/terminal"

class LavinMQCtl
  class TUI
    record Rect, x : Int32, y : Int32, width : Int32, height : Int32 do
      def right : Int32
        x + width - 1
      end

      def bottom : Int32
        y + height - 1
      end

      def inner_x : Int32
        x + 1
      end

      def inner_y : Int32
        y + 1
      end

      def inner_width : Int32
        width - 2
      end

      def inner_height : Int32
        height - 2
      end
    end

    # *sort* is the column's API sort key, *descending* the order it starts in
    # A *status* column gets a dot colored by its value, like running or stopped
    record Column, title : String, width : Int32, sort : String?, descending : Bool, value : Proc(JSON::Any, String), status : Bool = false

    record Table, title : String, path : String, columns : Array(Column), sort : String? = nil

    # Where the user is in a table page
    class TableState
      property cursor = 0
      property total = 0
      property sort : String?
      property? descending : Bool
      property filter = ""
      # The row shown with all its fields, found by its id after a refresh
      property detail : JSON::Any?
      property detail_id = ""
      property detail_scroll = 0
      property? detail_stale = false

      def initialize(@sort : String?, @descending : Bool)
      end
    end

    PAGES = [
      {key: '1', name: :overview, label: "Overview", nav: "Ovr"},
      {key: '2', name: :queues, label: "Queues", nav: "Q"},
      {key: '3', name: :connections, label: "Connections", nav: "Conn"},
      {key: '4', name: :channels, label: "Channels", nav: "Chan"},
      {key: '5', name: :exchanges, label: "Exchanges", nav: "Ex"},
      {key: '6', name: :consumers, label: "Consumers", nav: "Cons"},
      {key: '7', name: :vhosts, label: "Vhosts", nav: "Vh"},
      {key: '8', name: :nodes, label: "Nodes", nav: "Nodes"},
      {key: '9', name: :parameters, label: "Parameters", nav: "Param"},
      {key: '0', name: :policies, label: "Policies", nav: "Pol"},
      {key: 's', name: :shovels, label: "Shovels", nav: "Shov"},
      {key: 'f', name: :federation, label: "Federation", nav: "Fed"},
      {key: 'u', name: :users, label: "Users", nav: "Users"},
    ]

    HELP = {
      {"1-9 0 s f u", "Switch page, also Tab, Shift-Tab, Left, Right"},
      {"Up Down j k", "Move the selection"},
      {"PgUp PgDn", "Previous or next page of rows"},
      {"Home End g G", "First or last row"},
      {"Enter", "All fields of the selected row, Esc closes"},
      {"o", "Sort by the next column"},
      {"r", "Reverse the sort order"},
      {"/", "Filter by name, Esc clears the filter"},
      {"p Space", "Pause or resume refreshing"},
      {"?", "Show or hide this help"},
      {"q Ctrl-C", "Quit"},
    }

    # The dark theme of the management UI and lavinmq.com
    BG        = Color.rgb(0x18, 0x18, 0x18)
    BAR_BG    = Color.rgb(0x14, 0x14, 0x14)
    PANEL_BG  = Color.rgb(0x1D, 0x1D, 0x1D)
    ROW_BG    = Color.rgb(0x24, 0x24, 0x24)
    SELECT_BG = Color.rgb(0x2D, 0x2C, 0x2C)
    BORDER_FG = Color.rgb(0x41, 0x40, 0x40)
    GRID_FG   = Color.rgb(0x3A, 0x39, 0x39)
    TEXT_FG   = Color.rgb(0xCD, 0xCB, 0xC9)
    MUTED_FG  = Color.rgb(0x9D, 0x9C, 0x9A)
    WHITE     = Color.rgb(0xFA, 0xFA, 0xFA)
    DARK      = Color.rgb(0x14, 0x14, 0x14)
    # Its accent and first chart series, its second chart series, warning and danger
    GREEN  = Color.rgb(0x54, 0xBE, 0x7E)
    BLUE   = Color.rgb(0x45, 0x89, 0xFF)
    YELLOW = Color.rgb(0xE2, 0xB1, 0x49)
    RED    = Color.rgb(0xED, 0x43, 0x37)
    # Blocks filling a cell from the bottom, an eighth at a time, like the
    # management UI's filled charts
    GRAPH_FILL = {'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
    # Braille cells with one row of dots, from the bottom
    GRAPH_LINE = {'⣀', '⠤', '⠒', '⠉'}

    # Waits at least *interval* between refreshes, and long enough that the
    # broker spends at most a tenth of its time answering the TUI, up to 30s
    def self.refresh_delay(interval : Time::Span, fetch_time : Time::Span) : Time::Span
      {interval, {fetch_time * 10, 30.seconds}.min}.max
    end

    # *reconnect* opens a new connection after a timeout, for clients that
    # can't reconnect by themselves, like one on the control socket
    def initialize(@client : HTTP::Client, @interval : Float64 = 1.0, @screen : Screen = TerminalScreen.new, @reconnect : Proc(HTTP::Client)? = nil)
      @running = true
      @closed = false
      @width = 0
      @height = 0
      @page = :overview
      @last_error = nil.as(String?)
      @overview = nil.as(JSON::Any?)
      @items = [] of JSON::Any
      # The row number of the first of @items
      @items_first = 0
      @nodes = [] of JSON::Any
      @fetched = ""
      @fetch_time = Time::Span.zero
      @paused = false
      @help = false
      @input = nil.as(String?)
      @states = {} of Symbol => TableState
      @tables = {} of Symbol => Table
      @publish_history = [] of Float64
      @deliver_history = [] of Float64
      @ready_history = [] of Float64
      @unacked_history = [] of Float64
      @tables = tables
    end

    def start
      @width, @height = @screen.size
      refresh
      next_refresh = Time.instant + refresh_delay

      while @running
        timeout = @paused ? 1.hour : {next_refresh - Time.instant, Time::Span.zero}.max
        if event = @screen.poll_event(timeout)
          handle(event)
          # Take what queued up, like a held down key, before updating the screen
          100.times do
            break unless @running && (event = @screen.poll_event(Time::Span.zero))
            handle(event)
          end
          update if @running
        end

        if @running && !@paused && Time.instant >= next_refresh
          refresh
          next_refresh = Time.instant + refresh_delay
        end
      end
    ensure
      @screen.close
    end

    private def refresh_delay : Time::Span
      TUI.refresh_delay(@interval.seconds, @fetch_time)
    end

    private def handle(event : Event)
      case event
      in ResizeEvent
        @width = event.width
        @height = event.height
        @screen.sync
      in KeyEvent
        handle_key(event)
      end
    end

    private def handle_key(event : KeyEvent)
      return @running = false if event.key.ctrl_c?
      if input = @input
        return edit_filter(input, event)
      end
      if @help
        @help = false
        return unless event.key.char? && event.char == 'q'
      end
      if (state = table_state) && state.detail
        return if detail_key(state, event)
      end

      if event.key.char?
        handle_char(event.char)
      else
        navigate(event.key)
      end
    end

    private def navigate(key : Key)
      case key
      when .up?               then move(-1)
      when .down?             then move(1)
      when .page_up?          then move(-table_rows)
      when .page_down?        then move(table_rows)
      when .home?             then move_to(0)
      when .end?              then move_to(Int32::MAX)
      when .tab?, .right?     then switch_page(1)
      when .back_tab?, .left? then switch_page(-1)
      when .escape?           then set_filter("")
      when .enter?            then open_detail
      end
    end

    private def open_detail
      return unless state = table_state
      index = state.cursor - @items_first
      if index >= 0 && (item = @items[index]?)
        state.detail = item
        state.detail_id = item_id(item)
        state.detail_scroll = 0
        state.detail_stale = false
      end
    end

    # Scrolls or closes the details, other keys work as on the table
    private def detail_key(state : TableState, event : KeyEvent) : Bool
      key = event.key.char? ? VI_KEYS[event.char]? : event.key
      case key
      when Key::Up       then state.detail_scroll -= 1
      when Key::Down     then state.detail_scroll += 1
      when Key::PageUp   then state.detail_scroll -= table_rows
      when Key::PageDown then state.detail_scroll += table_rows
      when Key::Home     then state.detail_scroll = 0
      when Key::End      then state.detail_scroll = Int32::MAX
      when Key::Escape, Key::Enter, Key::Backspace
        state.detail = nil
      else
        return false
      end
      true
    end

    # What tells rows apart across refreshes, when the sort order moves them
    ID_KEYS = {"vhost", "name", "component", "upstream", "resource", "consumer_tag", "queue", "channel_details"}

    private def item_id(item : JSON::Any) : String
      ID_KEYS.join('\0') { |key| Fields.text(item, key, default: "") }
    end

    VI_KEYS = {'j' => Key::Down, 'k' => Key::Up, 'g' => Key::Home, 'G' => Key::End}

    private def handle_char(char : Char)
      if key = VI_KEYS[char]?
        return navigate(key)
      end
      case char
      when 'q'      then @running = false
      when 'o'      then next_sort
      when 'r'      then reverse_sort
      when 'p', ' ' then @paused = !@paused
      when '?'      then @help = true
      when '/'      then @input = table_state.try(&.filter)
      else
        if page = PAGES.find { |p| p[:key] == char }
          @page = page[:name]
        end
      end
    end

    private def edit_filter(input : String, event : KeyEvent)
      case event.key
      when .enter?
        @input = nil
        set_filter(input.strip)
      when .escape?    then @input = nil
      when .backspace? then @input = input.rchop
      when .char?
        @input = input + event.char if input.size < 100 && event.char.printable?
      end
    end

    private def set_filter(filter : String)
      if state = table_state
        state.filter = filter
        state.cursor = 0
      end
    end

    private def move(delta : Int32)
      if state = table_state
        move_to(state.cursor + delta)
      end
    end

    private def move_to(index : Int32)
      if state = table_state
        state.cursor = index.clamp(0, {state.total - 1, 0}.max)
      end
    end

    private def next_sort
      return unless state = table_state
      columns = @tables[@page].columns.select(&.sort)
      return if columns.empty?
      index = columns.index { |c| c.sort == state.sort }
      column = columns[index ? (index + 1) % columns.size : 0]
      state.sort = column.sort
      state.descending = column.descending
      state.cursor = 0
    end

    private def reverse_sort
      if (state = table_state) && state.sort
        state.descending = !state.descending?
        state.cursor = 0
      end
    end

    private def switch_page(delta : Int32)
      index = PAGES.index! { |p| p[:name] == @page }
      @page = PAGES[(index + delta) % PAGES.size][:name]
    end

    private def table_state : TableState?
      return if @page == :overview
      @states[@page] ||= begin
        sort = @tables[@page].sort
        TableState.new(sort, @tables[@page].columns.find { |c| c.sort == sort }.try(&.descending) || false)
      end
    end

    # Fetches only when what's shown needs other data than what's fetched
    private def update
      wanted_fetch == @fetched ? draw : refresh
    end

    private def wanted_fetch : String
      if state = table_state
        "#{@page} #{state.cursor // table_rows} #{table_rows} #{state.sort} #{state.descending?} #{state.filter}"
      else
        "overview #{hot_queue_rows}"
      end
    end

    private def refresh
      started = Time.instant
      @last_error = nil
      @width, @height = @screen.size
      if @page == :overview || @overview.nil?
        if overview = @overview = fetch_json("/api/overview", "overview")
          update_histories(overview)
        end
      end
      if @page == :overview
        rows = hot_queue_rows
        @items = rows > 0 ? fetch_page("/api/queues", "queues", 1, rows, "messages", true, "")[0] : [] of JSON::Any
        @items_first = 0
        @nodes = fetch_list("/api/nodes", "nodes")
      else
        fetch_table
        update_detail
      end
      @fetched = wanted_fetch
      @fetch_time = Time.instant - started
      draw
    end

    private def fetch_table
      return unless state = table_state
      table = @tables[@page]
      rows = table_rows
      # Once more if the rows shrank and the cursor ended up past the end
      2.times do
        page = state.cursor // rows + 1
        @items_first = (page - 1) * rows
        if @page == :nodes
          nodes = node_rows(fetch_list(table.path, "nodes"))
          nodes.select! { |node| Fields.text(node, "name").includes?(state.filter) } unless state.filter.empty?
          @items = nodes[(page - 1) * rows, rows]? || [] of JSON::Any
          state.total = nodes.size
        else
          label = table.title.downcase
          @items, state.total = fetch_page(table.path, label, page, rows, state.sort, state.descending?, state.filter)
        end
        last = {state.total - 1, 0}.max
        break if state.cursor <= last
        state.cursor = last
      end
    end

    private def fetch_page(path : String, label : String, page : Int32, size : Int32, sort : String?, descending : Bool, filter : String) : {Array(JSON::Any), Int32}
      params = URI::Params.new
      params["page"] = page.to_s
      params["page_size"] = size.to_s
      if sort
        params["sort"] = sort
        params["sort_reverse"] = descending.to_s
      end
      params["name"] = filter unless filter.empty?
      data = fetch_json("#{path}?#{params}", label)
      if items = Fields.dig(data, "items").try(&.as_a?)
        {items, Fields.int(data, "filtered_count").clamp(0, Int32::MAX).to_i32}
      elsif items = data.try(&.as_a?)
        # Not paginated by the API
        items = items.select { |item| Fields.text(item, "name").includes?(filter) } unless filter.empty?
        {items[(page - 1) * size, size]? || [] of JSON::Any, items.size}
      else
        record_error("#{label}: missing items") if data
        {[] of JSON::Any, 0}
      end
    end

    private def fetch_list(path : String, label : String) : Array(JSON::Any)
      data = fetch_json(path, label)
      data.try(&.as_a?) || Fields.dig(data, "items").try(&.as_a?) || [] of JSON::Any
    end

    private def fetch_json(path : String, label : String, retry = true) : JSON::Any?
      if @closed && (reconnect = @reconnect)
        @client = reconnect.call
        @closed = false
      end
      response = @client.get(path)
      unless response.status_code == 200
        record_error("#{label}: HTTP #{response.status_code} #{response.status}")
        return
      end
      JSON.parse(response.body)
    rescue ex : JSON::ParseException
      record_error("#{label}: invalid JSON (#{ex.message})")
      nil
    rescue ex
      # The connection is broken, or still open after a timeout and the late
      # response would be read as the answer to the next request. A TCP client
      # reconnects by itself, @reconnect opens a new one for the next request.
      @client.close
      @closed = true
      # A TCP client retries once on a new connection when the server has
      # closed the one it had, a client on the control socket raises instead
      if retry && @reconnect && !ex.is_a?(IO::Error)
        return fetch_json(path, label, retry: false)
      end
      record_error("#{label}: #{ex.message || ex.class.name}")
      nil
    end

    # Keeps the first error of a refresh, later ones are often caused by it
    private def record_error(message : String)
      @last_error ||= message
    end

    private def update_detail
      return unless (state = table_state) && state.detail
      if item = @items.find { |i| item_id(i) == state.detail_id }
        state.detail = item
        state.detail_stale = false
      else
        state.detail_stale = true
      end
    end

    # The node, and a row for each of its followers
    private def node_rows(nodes : Array(JSON::Any)) : Array(JSON::Any)
      nodes.flat_map do |node|
        followers = Fields.dig(node, "followers").try(&.as_a?) || [] of JSON::Any
        role = followers.empty? ? "standalone" : "leader"
        rows = [with_field(node, "role", role)]
        followers.each do |follower|
          address = Fields.text(follower, "remote_address")
          rows << with_field(with_field(follower, "name", address), "role", "follower")
        end
        rows
      end
    end

    private def with_field(item : JSON::Any, key : String, value : String) : JSON::Any
      hash = item.as_h?.try(&.dup) || {} of String => JSON::Any
      hash[key] = JSON::Any.new(value)
      JSON::Any.new(hash)
    end

    private def draw
      @screen.clear
      @width, @height = @screen.size
      fill_rect(0, 0, @width, @height, bg: BG)
      draw_header
      if @page == :overview
        draw_overview
      else
        draw_table
      end
      draw_footer
      draw_help if @help
      @screen.render
    end

    private def draw_header
      fill_rect(0, 0, @width, 1, bg: BAR_BG)
      delay = refresh_delay
      status = if @paused
                 " PAUSED "
               elsif delay > @interval.seconds
                 " refresh every %.1fs " % delay.total_seconds
               else
                 ""
               end
      right = @width - Text.width(status)
      x = print_at(0, 0, " LAVINMQ ", WHITE, BAR_BG, true, right)
      x += print_at(x, 0, " #{PAGES.find! { |p| p[:name] == @page }[:label]} ", WHITE, SELECT_BG, true, right - x)
      version = Fields.text(@overview, "lavinmq_version", default: "?")
      x += 1 + print_at(x + 1, 0, " v#{version} ", GREEN, BAR_BG, max_width: right - x - 1)
      print_at(x, 0, " #{Fields.text(@overview, "node", default: "?")}", MUTED_FG, BAR_BG, max_width: right - x)
      print_at(right, 0, status, YELLOW, BAR_BG, true)
    end

    private def draw_footer
      y = @height - 1
      return if y < 1
      fill_rect(0, y, @width, 1, bg: BAR_BG)
      if input = @input
        title = @tables[@page].title.downcase
        x = print_at(0, y, " Filter #{title} by name ", MUTED_FG, BAR_BG)
        x += print_at(x, y, input, WHITE, BAR_BG, true)
        x += print_at(x, y, "▏", GREEN, BAR_BG)
        print_at(x + 1, y, "Enter applies, Esc cancels", MUTED_FG, BAR_BG)
        return
      end

      error_width = 0
      if error = @last_error
        error_text = " #{error} "
        error_width = {Text.width(error_text), @width // 2}.min
        print_fit(@width - error_width, y, error_text, error_width, RED, BAR_BG, true)
      end
      draw_nav(y, @width - error_width)
    end

    NAV_EXTRAS = [{key: '?', label: "Help", nav: "Help"}, {key: 'q', label: "Quit", nav: "Quit"}]

    # The keys to the pages, like the management UI's menu, with the longest
    # labels that fit in *width*
    private def draw_nav(y : Int32, width : Int32)
      items = PAGES.map { |p| {p[:key], p[:label], p[:nav], p[:name] == @page} } +
              NAV_EXTRAS.map { |p| {p[:key], p[:label], p[:nav], false} }
      long = items.sum { |item| item[1].size + 3 } + 1
      short = items.sum { |item| item[2].size + 3 } + 1
      x = 0
      items.each do |(key, label, nav, current)|
        text = long <= width ? label : (short <= width ? nav : "")
        bg = current ? SELECT_BG : BAR_BG
        x += print_at(x, y, " #{key}", GREEN, bg, true, width - x)
        x += print_at(x, y, " #{text}", current ? WHITE : MUTED_FG, bg, current, width - x) unless text.empty?
        x += print_at(x, y, " ", WHITE, bg, max_width: width - x) if current
      end
    end

    # Between the header and the footer, with a margin around it
    private def draw_help
      width = {64, @width - 4}.min
      height = {HELP.size + 4, @height - 5}.min
      rect = Rect.new((@width - width) // 2, {(@height - height) // 2, 2}.max, width, height)
      # A margin to set it apart from the panel under it
      fill_rect(rect.x - 1, rect.y - 1, rect.width + 2, rect.height + 2, bg: BG)
      draw_panel(rect, "Keys")
      HELP.each_with_index do |(keys, text), i|
        y = rect.inner_y + 1 + i
        break if y >= rect.bottom
        print_fit(rect.inner_x + 2, y, keys, 16, GREEN, PANEL_BG, true)
        print_fit(rect.inner_x + 19, y, text, rect.inner_width - 20, TEXT_FG, PANEL_BG)
      end
    end

    # Rows that fit below a table's header, see draw_table
    private def table_rows : Int32
      {@height - 8, 1}.max
    end

    private def full_overview? : Bool
      @width >= 100 && @height >= 32
    end

    # Rows in the busiest queues panel, see draw_overview
    private def hot_queue_rows : Int32
      full_overview? ? {@height - 30, 1}.max : 0
    end

    private def draw_table
      return unless state = table_state
      if detail = state.detail
        return draw_detail(state, detail)
      end
      table = @tables[@page]
      rect = Rect.new(1, 2, @width - 2, @height - 4)
      draw_panel(rect, table.title, table_note(state), state.total.to_s)

      x = rect.inner_x + 2
      width = rect.inner_width - 4
      draw_row(rect.inner_y + 1, table_headers(table, state), table.columns, WHITE, PANEL_BG, true, x, width)
      if @items.empty?
        print_at(x, rect.inner_y + 3, state.filter.empty? ? "No data" : "Nothing matches the filter", MUTED_FG, PANEL_BG)
        return
      end

      # The rows fetched start at @items_first
      @items.each_with_index do |item, i|
        y = rect.inner_y + 2 + i
        break if y >= rect.bottom
        selected = @items_first + i == state.cursor
        bg = selected ? SELECT_BG : (i.even? ? PANEL_BG : ROW_BG)
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        set_cell(rect.inner_x + 1, y, '▌', GREEN, bg) if selected
        values = table.columns.map(&.value.call(item))
        draw_row(y, values, table.columns, selected ? WHITE : TEXT_FG, bg, selected, x, width, dots: true)
      end
    end

    private def draw_detail(state : TableState, item : JSON::Any)
      rect = Rect.new(1, 2, @width - 2, @height - 4)
      fields = detail_fields(item)
      key_width = {fields.max_of? { |(key, _)| Text.width(key) } || 0, rect.inner_width // 3}.min
      value_width = rect.inner_width - key_width - 6
      lines = fields.flat_map do |(key, value)|
        wrap(value, value_width).map_with_index { |line, i| {i.zero? ? key : "", line} }
      end
      rows = {rect.inner_height - 2, 0}.max
      state.detail_scroll = state.detail_scroll.clamp(0, {lines.size - rows, 0}.max)

      name = Fields.text(item, "name", default: Fields.text(item, "consumer_tag", default: Fields.text(item, "upstream")))
      note = String.build do |s|
        if lines.size > rows
          s << "lines " << state.detail_scroll + 1 << "-" << {state.detail_scroll + rows, lines.size}.min << " of " << lines.size
        end
        s << "  not on this page anymore" if state.detail_stale?
      end
      draw_panel(rect, "#{@tables[@page].title} › #{name.presence || "(default)"}", note.lstrip)
      lines.skip(state.detail_scroll).first(rows).each_with_index do |(key, value), i|
        y = rect.inner_y + 1 + i
        print_fit(rect.inner_x + 2, y, key, key_width, MUTED_FG, PANEL_BG)
        print_fit(rect.inner_x + 4 + key_width, y, value, value_width, WHITE, PANEL_BG)
      end
    end

    # Every field, nested ones with dotted keys and rates next to their counts
    private def detail_fields(value : JSON::Any, prefix = "", fields = [] of {String, String}) : Array({String, String})
      if hash = value.as_h?
        hash.each do |key, field|
          next if key.ends_with?("_details") && hash.has_key?(key.rchop("_details"))
          if field.as_h?.try(&.empty?) == false || field.as_a?.try(&.any? { |v| v.as_h? || v.as_a? })
            detail_fields(field, "#{prefix}#{key}.", fields)
            next
          end
          fields << {"#{prefix}#{key}", detail_value(hash, key, field)}
        end
      elsif array = value.as_a?
        array.each_with_index { |field, i| detail_fields(JSON::Any.new({i.to_s => field}), prefix, fields) }
      end
      fields
    end

    # With its size if it counts bytes, and its rate if it has one
    private def detail_value(hash : Hash(String, JSON::Any), key : String, field : JSON::Any) : String
      return "(hidden)" if key == "password_hash"
      text = detail_text(field)
      bytes = key.includes?("bytes") || key.ends_with?("_oct")
      if bytes && (count = field.as_i64?) && count >= 1024
        text += " (#{Fields.human_bytes(count)})"
      end
      details = hash["#{key}_details"]?
      return text unless Fields.dig(details, "rate")
      text + (bytes ? " (#{Fields.bytes_rate(details, "rate")})" : " (#{Fields.rate(details, "rate")}/s)")
    end

    private def detail_text(value : JSON::Any) : String
      if array = value.as_a?
        array.join(", ") { |v| Fields.redact_uris(Fields.text(v)) }
      else
        Fields.redact_uris(Fields.text(value))
      end
    end

    # Lines at most *width* cells wide, up to a screenful
    private def wrap(text : String, width : Int32) : Array(String)
      return [text] if width < 1 || Text.width(text) <= width
      lines = [] of String
      line = String::Builder.new
      used = 0
      text.each_char do |char|
        char_width = Text.width(Text.sanitize(char))
        if used + char_width > width
          lines << line.to_s
          return lines if lines.size >= table_rows
          line = String::Builder.new
          used = 0
        end
        line << char
        used += char_width
      end
      lines << line.to_s
    end

    private def table_note(state : TableState) : String
      String.build do |s|
        s << @items_first + 1 << "-" << @items_first + @items.size << " of " << state.total unless @items.empty?
        s << "  filter \"" << state.filter << '"' unless state.filter.empty?
      end
    end

    # The sorted column gets an arrow for the sort order
    private def table_headers(table : Table, state : TableState) : Array(String)
      table.columns.map do |column|
        next column.title unless column.sort && column.sort == state.sort
        "#{column.title} #{state.descending? ? '↓' : '↑'}"
      end
    end

    # The last column gets the width that's left
    private def draw_row(y : Int32, values : Array(String), columns : Array(Column), fg : Color, bg : Color, bold : Bool, x : Int32, max_width : Int32, dots = false)
      used = 0
      columns.each_with_index do |column, i|
        break if used >= max_width
        width = i == columns.size - 1 ? max_width - used : {column.width, max_width - used}.min
        if dots && column.status && width > 2 && values[i] != "-"
          print_at(x + used, y, "●", state_color(values[i]), bg)
          set_cell(x + used + 1, y, ' ', fg, bg)
          print_fit(x + used + 2, y, values[i], width - 2, fg, bg, bold)
        else
          print_fit(x + used, y, values[i], width, fg, bg, bold)
        end
        used += width + 1
      end
    end

    private def state_color(state : String) : Color
      case state.downcase
      when "running", "live", "up"                                       then GREEN
      when "starting", "flow", "paused", "blocking", "blocked", "idle"   then YELLOW
      when "error", "stopped", "terminated", "closed", "closing", "down" then RED
      else                                                                    MUTED_FG
      end
    end

    private def draw_overview
      unless overview = @overview
        draw_panel(Rect.new(1, 2, @width - 2, 5), "Error", border: RED)
        print_at(3, 4, "Overview unavailable", RED, PANEL_BG, true)
        return
      end

      unless full_overview?
        draw_compact_overview(overview)
        return
      end

      left_width = (@width // 3).clamp(34, 42)
      right_width = @width - left_width - 3
      draw_totals_panel(Rect.new(1, 2, left_width, 9), overview)
      draw_messages_panel(Rect.new(1, 11, left_width, 10), overview)
      draw_node_panel(Rect.new(1, 21, left_width, @height - 23), overview, @nodes.first?)
      draw_rate_graph(Rect.new(left_width + 2, 2, right_width, 10), overview)
      draw_queue_graph(Rect.new(left_width + 2, 13, right_width, 10), overview)
      draw_hot_queues(Rect.new(left_width + 2, 24, right_width, @height - 26))
    end

    # Totals and messages side by side when there's room, graphs below if they fit
    private def draw_compact_overview(overview : JSON::Any)
      width = @width - 2
      if width >= 68
        half = width // 2
        draw_totals_panel(Rect.new(1, 2, half, 10), overview)
        draw_messages_panel(Rect.new(half + 2, 2, width - half - 1, 10), overview)
        y = 12
      else
        draw_totals_panel(Rect.new(1, 2, width, 9), overview)
        draw_messages_panel(Rect.new(1, 11, width, 10), overview)
        y = 21
      end
      remaining = @height - 1 - y
      if remaining >= 16
        draw_rate_graph(Rect.new(1, y, width, remaining // 2), overview)
        draw_queue_graph(Rect.new(1, y + remaining // 2, width, remaining - remaining // 2), overview)
      elsif remaining >= 6
        draw_rate_graph(Rect.new(1, y, width, remaining), overview)
      end
    end

    private def draw_totals_panel(rect : Rect, overview : JSON::Any)
      draw_panel(rect, "Totals")
      totals = {
        {"Connections", "connections"}, {"Channels", "channels"},
        {"Queues", "queues"}, {"Consumers", "consumers"},
        {"Exchanges", "exchanges"}, {"Bindings", "bindings"},
      }
      column_width = (rect.inner_width - 4) // 2
      totals.each_with_index do |(label, key), i|
        x = rect.inner_x + 2 + (i % 2) * column_width
        y = rect.inner_y + 1 + (i // 2) * 2
        print_at(x, y, label, MUTED_FG, PANEL_BG, max_width: column_width - 2)
        print_at(x, y + 1, Fields.text(overview, "object_totals", key), WHITE, PANEL_BG, true, column_width - 2)
      end
    end

    private def draw_messages_panel(rect : Rect, overview : JSON::Any)
      draw_panel(rect, "Messages")
      total = Fields.float(overview, "queue_totals", "messages")
      ready = Fields.float(overview, "queue_totals", "messages_ready")
      unacked = Fields.float(overview, "queue_totals", "messages_unacknowledged")
      x = rect.inner_x + 2
      y = rect.inner_y + 1
      print_at(x, y, "Total", MUTED_FG, PANEL_BG)
      print_at(x + 13, y, Fields.int(overview, "queue_totals", "messages").to_s, WHITE, PANEL_BG, true)
      print_at(x, y + 1, "Publish", MUTED_FG, PANEL_BG)
      print_at(x + 13, y + 1, Fields.rate(overview, "message_stats", "publish_details", "rate") + "/s", GREEN, PANEL_BG, true)
      print_at(x, y + 2, "Deliver", MUTED_FG, PANEL_BG)
      print_at(x + 13, y + 2, Fields.rate(overview, "message_stats", "deliver_get_details", "rate") + "/s", BLUE, PANEL_BG, true)
      draw_bar(x, y + 4, rect.inner_width - 4, "Ready", ready, total, GREEN)
      draw_bar(x, y + 6, rect.inner_width - 4, "Unacked", unacked, total, BLUE)
    end

    private def draw_node_panel(rect : Rect, overview : JSON::Any, node : JSON::Any?)
      draw_panel(rect, "Node")
      x = rect.inner_x + 2
      y = rect.inner_y + 1
      width = rect.inner_width - 4
      unless node
        print_at(x, y, "No node data", MUTED_FG, PANEL_BG)
        return
      end

      print_at(x, y, Fields.text(node, "name"), WHITE, PANEL_BG, true, width)
      print_at(x, y + 1, "Uptime #{Fields.duration(node, "uptime")}", MUTED_FG, PANEL_BG, max_width: 17)
      print_at(x + 18, y + 1, "Sockets #{Fields.text(node, "sockets_used")}", MUTED_FG, PANEL_BG, max_width: width - 18)
      mem_used = Fields.float(node, "mem_used")
      mem_limit = Fields.float(node, "mem_limit")
      draw_bar(x, y + 3, width, "Memory", mem_used, positive_or(mem_limit, mem_used), usage_color(mem_used, mem_limit), bytes: true)
      return if y + 4 >= rect.bottom
      disk_total = Fields.float(node, "disk_total")
      disk_free = Fields.float(node, "disk_free")
      if disk_total > 0
        draw_bar(x, y + 4, width, "Disk used", disk_total - disk_free, disk_total, usage_color(disk_total - disk_free, disk_total), bytes: true)
      else
        print_at(x, y + 4, "Disk free #{Fields.bytes(node, "disk_free")}", MUTED_FG, PANEL_BG, max_width: width)
      end
      return if y + 5 >= rect.bottom
      fd_used = Fields.float(node, "fd_used")
      fd_total = Fields.float(node, "fd_total")
      draw_bar(x, y + 5, width, "FD", fd_used, positive_or(fd_total, fd_used), usage_color(fd_used, fd_total))
      return if y + 7 >= rect.bottom
      recv = Fields.bytes_rate(overview, "recv_oct_details", "rate")
      send = Fields.bytes_rate(overview, "send_oct_details", "rate")
      print_at(x, y + 7, "Network", MUTED_FG, PANEL_BG)
      print_at(x + 8, y + 7, "in #{recv} out #{send}", TEXT_FG, PANEL_BG, max_width: width - 8)
      return if y + 8 >= rect.bottom
      followers = Fields.dig(node, "followers").try(&.as_a?) || [] of JSON::Any
      cluster = if followers.empty?
                  "single node"
                else
                  lag = followers.max_of { |f| Fields.int(f, "lag_in_bytes") }
                  "#{followers.size} follower#{"s" if followers.size > 1}, lag #{Fields.human_bytes(lag)}"
                end
      print_at(x, y + 8, "Cluster", MUTED_FG, PANEL_BG)
      print_at(x + 8, y + 8, cluster, TEXT_FG, PANEL_BG, max_width: width - 8)
    end

    private def draw_rate_graph(rect : Rect, overview : JSON::Any)
      publish = Fields.rate(overview, "message_stats", "publish_details", "rate")
      deliver = Fields.rate(overview, "message_stats", "deliver_get_details", "rate")
      draw_panel(rect, "Message rates")
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      max = draw_graph(graph, @publish_history, GREEN, @deliver_history, BLUE)
      draw_legend(rect, {"Publish #{publish}/s", "Deliver #{deliver}/s"}, "max %.1f/s" % max)
    end

    private def draw_queue_graph(rect : Rect, overview : JSON::Any)
      ready = Fields.int(overview, "queue_totals", "messages_ready")
      unacked = Fields.int(overview, "queue_totals", "messages_unacknowledged")
      draw_panel(rect, "Queued messages")
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      max = draw_graph(graph, @ready_history, GREEN, @unacked_history, BLUE)
      draw_legend(rect, {"Ready #{ready}", "Unacked #{unacked}"}, "max #{Fields.to_i64(max)}")
    end

    # The green and blue series' names with chips in their colors, like the
    # management UI's chart legends, and the scale on the right
    private def draw_legend(panel : Rect, names : {String, String}, scale : String)
      y = panel.bottom - 1
      right = panel.right - 2 - scale.size
      x = panel.inner_x + 2
      {GREEN, BLUE}.each_with_index do |color, i|
        x += print_at(x, y, "■ ", color, PANEL_BG, max_width: right - x)
        x += print_at(x, y, names[i], TEXT_FG, PANEL_BG, max_width: right - x - 1) + 3
      end
      print_at(right, y, scale, MUTED_FG, PANEL_BG) if right > panel.inner_x + 2
    end

    private def draw_hot_queues(rect : Rect)
      draw_panel(rect, "Busiest queues")
      x = rect.inner_x + 2
      width = rect.inner_width - 4
      columns = @tables[:queues].columns.reject(&.title.in?("Vhost", "State"))
      draw_row(rect.inner_y + 1, columns.map(&.title), columns, WHITE, PANEL_BG, true, x, width)
      @items.each_with_index do |queue, i|
        y = rect.inner_y + 2 + i
        break if y >= rect.bottom
        bg = i.even? ? PANEL_BG : ROW_BG
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        draw_row(y, columns.map(&.value.call(queue)), columns, TEXT_FG, bg, false, x, width)
      end
    end

    # A box with rounded corners, with *title* and *badge* like the management
    # UI's headings and count badges, and *note* after them
    private def draw_panel(rect : Rect, title : String, note = "", badge = "", border = BORDER_FG)
      return if rect.width <= 1 || rect.height <= 1

      fill_rect(rect.x, rect.y, rect.width, rect.height, bg: PANEL_BG)
      rect.width.times do |i|
        set_cell(rect.x + i, rect.y, '─', border, PANEL_BG)
        set_cell(rect.x + i, rect.bottom, '─', border, PANEL_BG)
      end
      rect.height.times do |i|
        set_cell(rect.x, rect.y + i, '│', border, PANEL_BG)
        set_cell(rect.right, rect.y + i, '│', border, PANEL_BG)
      end
      set_cell(rect.x, rect.y, '╭', border, PANEL_BG)
      set_cell(rect.right, rect.y, '╮', border, PANEL_BG)
      set_cell(rect.x, rect.bottom, '╰', border, PANEL_BG)
      set_cell(rect.right, rect.bottom, '╯', border, PANEL_BG)
      right = rect.right - 1
      x = rect.x + 2
      x += print_at(x, rect.y, " #{title} ", WHITE, PANEL_BG, true, right - x)
      x += print_at(x, rect.y, " #{badge} ", DARK, GREEN, true, right - x) unless badge.empty?
      print_at(x, rect.y, " #{note} ", MUTED_FG, PANEL_BG, max_width: right - x) unless note.empty?
    end

    # Green, or yellow and red as *value* gets close to *limit*, if there is one
    private def usage_color(value : Float64, limit : Float64) : Color
      return GREEN unless limit > 0
      fraction = value / limit
      fraction >= 0.9 ? RED : (fraction >= 0.75 ? YELLOW : GREEN)
    end

    private def draw_bar(x : Int32, y : Int32, width : Int32, label : String, value : Float64, max : Float64, color : Color, bytes = false)
      return if width <= 0

      label_width = {label.size + 1, 10}.max
      value_text = bytes ? Fields.human_bytes(Fields.to_i64(value)) : Fields.to_i64(value).to_s
      value_width = {value_text.size + 1, 8}.max
      bar_width = width - label_width - value_width
      return if bar_width <= 0

      filled = max > 0 ? (value / max * bar_width).round.clamp(0, bar_width).to_i : 0
      print_fit(x, y, label, label_width, MUTED_FG, PANEL_BG)
      bar_width.times do |i|
        if i < filled
          set_cell(x + label_width + i, y, '━', color, PANEL_BG, true)
        else
          set_cell(x + label_width + i, y, '·', GRID_FG, PANEL_BG)
        end
      end
      print_at(x + label_width + bar_width + 1, y, value_text, color, PANEL_BG, true, value_width - 1)
    end

    # Draws *area* as a filled graph and *line* as a line in front of it, on
    # a shared scale with the newest values to the right, so both stay
    # visible when they're equal. Returns the top of the scale.
    private def draw_graph(rect : Rect, area : Array(Float64), area_color : Color, line : Array(Float64), line_color : Color) : Float64
      return 0.0 if rect.width <= 0 || rect.height <= 0

      draw_graph_grid(rect)
      area = area.last(rect.width)
      line = line.last(rect.width)
      max = {area.max? || 0.0, line.max? || 0.0}.max
      return max unless max > 0.0

      area_levels = rect.height * GRAPH_FILL.size
      line_levels = rect.height * GRAPH_LINE.size
      rect.width.times do |i|
        x = rect.right - i
        full = 0
        if value = area[-1 - i]?
          full, partial = (value / max * area_levels).ceil.clamp(0, area_levels).to_i.divmod(GRAPH_FILL.size)
          full.times { |row| set_cell(x, rect.bottom - row, GRAPH_FILL[-1], area_color, PANEL_BG) }
          set_cell(x, rect.bottom - full, GRAPH_FILL[partial - 1], area_color, PANEL_BG) if partial > 0
        end
        if value = line[-1 - i]?
          # Zero is drawn on the bottom row, the line never disappears. Where
          # the area fills the cell the line is drawn on the area's color.
          level = (value / max * line_levels).ceil.clamp(1, line_levels).to_i
          row, dot = (level - 1).divmod(GRAPH_LINE.size)
          set_cell(x, rect.bottom - row, GRAPH_LINE[dot], line_color, row < full ? area_color : PANEL_BG)
        end
      end
      max
    end

    private def draw_graph_grid(rect : Rect)
      rect.height.times do |row|
        next unless row.even?
        rect.width.times do |col|
          next unless col % 4 == 0
          set_cell(rect.x + col, rect.y + row, '·', GRID_FG, PANEL_BG)
        end
      end
    end

    private def fill_rect(x : Int32, y : Int32, width : Int32, height : Int32, bg : Color = PANEL_BG)
      height.times do |dy|
        width.times do |dx|
          set_cell(x + dx, y + dy, ' ', TEXT_FG, bg)
        end
      end
    end

    private def set_cell(x : Int32, y : Int32, char : Char, fg : Color, bg : Color, bold = false)
      return if x < 0 || x >= @width || y < 0 || y >= @height
      @screen.set_cell(x, y, char, fg, bg, bold)
    end

    # The only way text reaches the screen: control characters are replaced,
    # zero width characters left out and wide characters take two cells.
    # Returns the number of cells used, at most *max_width*.
    private def print_at(x : Int32, y : Int32, text : String, fg : Color, bg : Color, bold = false, max_width = @width - x) : Int32
      return 0 if y < 0 || y >= @height
      max_width = {max_width, @width - x}.min
      used = 0
      text.each_char do |char|
        char = Text.sanitize(char)
        width = Text.width(char)
        next if width == 0
        break if used + width > max_width
        set_cell(x + used, y, char, fg, bg, bold) if x + used >= 0
        used += width
      end
      used
    end

    # Prints *text* in exactly *width* cells, cut with ".." or padded with spaces
    private def print_fit(x : Int32, y : Int32, text : String, width : Int32, fg : Color, bg : Color, bold = false)
      return if width <= 0
      used = if Text.width(text) <= width || width < 3
               print_at(x, y, text, fg, bg, bold, width)
             else
               cut = print_at(x, y, text, fg, bg, bold, width - 2)
               cut + print_at(x + cut, y, "..", fg, bg, bold, width - cut)
             end
      (used...width).each { |i| set_cell(x + i, y, ' ', fg, bg) }
    end

    private def positive_or(max : Float64, value : Float64) : Float64
      max > 0 ? max : value
    end

    private def update_histories(overview : JSON::Any)
      update_history(@publish_history, overview, "message_stats", "publish_details")
      update_history(@deliver_history, overview, "message_stats", "deliver_get_details")
      totals = Fields.dig(overview, "queue_totals")
      update_history(@ready_history, Fields.float(totals, "messages_ready"), Fields.floats(totals, "messages_ready_log"))
      update_history(@unacked_history, Fields.float(totals, "messages_unacknowledged"), Fields.floats(totals, "messages_unacknowledged_log"))
    end

    private def update_history(history : Array(Float64), overview : JSON::Any, stats : String, details : String)
      update_history(history, Fields.float(overview, stats, details, "rate"), Fields.floats(overview, stats, details, "log"))
    end

    # The broker's log of the value, or what the TUI has seen if it has none
    private def update_history(history : Array(Float64), current : Float64, log : Array(Float64))
      if log.empty?
        history << current
      else
        history.clear
        history.concat(log)
        history << current if history.last? != current
      end
      history.shift(history.size - 240) if history.size > 240
    end

    # Reading and formatting values from the API
    module Fields
      extend self

      # Matches the password in scheme://user:password@host
      URI_PASSWORD = %r{(//[^/:@]*):[^/@]*@}

      def dig(value : JSON::Any?, *keys) : JSON::Any?
        keys.each do |key|
          value = value.try(&.as_h?).try(&.[key]?)
        end
        value
      end

      def text(item : JSON::Any?, *keys, default = "-") : String
        value = dig(item, *keys)
        case raw = value.try(&.raw)
        when Nil         then default
        when String      then raw
        when Array, Hash then value.to_json
        else                  raw.to_s
        end
      end

      def float(item : JSON::Any?, *keys) : Float64
        dig(item, *keys).try(&.as_f?) || 0.0
      end

      def floats(item : JSON::Any?, *keys) : Array(Float64)
        dig(item, *keys).try(&.as_a?).try(&.compact_map(&.as_f?)) || [] of Float64
      end

      # Clamped, as the API may send values that don't fit
      def int(item : JSON::Any?, *keys) : Int64
        value = dig(item, *keys)
        value.try(&.as_i64?) || to_i64(value.try(&.as_f?) || 0.0)
      end

      def to_i64(value : Float64) : Int64
        value.finite? ? value.clamp(-9.0e18, 9.0e18).to_i64 : 0_i64
      end

      def rate(item : JSON::Any?, *keys) : String
        "%.1f" % float(item, *keys)
      end

      def bytes(item : JSON::Any?, *keys) : String
        human_bytes(int(item, *keys))
      end

      def bytes_rate(item : JSON::Any?, *keys) : String
        "#{human_bytes(int(item, *keys))}/s"
      end

      def bool(item : JSON::Any?, *keys) : String
        dig(item, *keys).try(&.as_bool?) ? "yes" : "-"
      end

      def duration(item : JSON::Any?, *keys) : String
        seconds = int(item, *keys) // 1000
        days, rest = seconds.divmod(86_400)
        hours, rest = rest.divmod(3600)
        return "#{days}d#{hours}h" if days > 0
        return "#{hours}h#{rest // 60}m" if hours > 0
        "#{rest // 60}m"
      end

      def uri(item : JSON::Any?, *keys) : String
        if uris = dig(item, *keys).try(&.as_a?)
          uris.join(", ") { |uri| redact_uris(text(uri)) }
        else
          redact_uris(text(item, *keys))
        end
      end

      # Shovel and federation URIs may embed credentials, don't put them on screen
      def redact_uris(text : String) : String
        text.gsub(URI_PASSWORD, "\\1:***@")
      end

      def human_bytes(bytes : Int64) : String
        units = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
        value = bytes.to_f
        unit = 0
        while value.abs >= 1024.0 && unit < units.size - 1
          value /= 1024.0
          unit += 1
        end
        unit.zero? ? "#{bytes}B" : "%.1f%s" % {value, units[unit]}
      end
    end

    private def col(title : String, width : Int32, sort : String? = nil, descending = false, status = false, &value : JSON::Any -> String) : Column
      Column.new(title, width, sort, descending, value, status)
    end

    private def tables : Hash(Symbol, Table)
      {
        :queues => Table.new("Queues", "/api/queues", [
          col("Vhost", 12, "vhost") { |q| Fields.text(q, "vhost") },
          col("Name", 30, "name") { |q| Fields.text(q, "name") },
          col("State", 10, "state", status: true) { |q| Fields.text(q, "state") },
          col("Msgs", 9, "messages", true) { |q| Fields.text(q, "messages") },
          col("Ready", 9, "messages_ready", true) { |q| Fields.text(q, "messages_ready") },
          col("Unacked", 9, "messages_unacknowledged", true) { |q| Fields.text(q, "messages_unacknowledged") },
          col("Cons", 6, "consumers", true) { |q| Fields.text(q, "consumers") },
          col("Pub/s", 9, "message_stats.publish_details.rate", true) { |q| Fields.rate(q, "message_stats", "publish_details", "rate") },
          col("Deliver/s", 10, "message_stats.deliver_get_details.rate", true) { |q| Fields.rate(q, "message_stats", "deliver_get_details", "rate") },
        ], sort: "messages"),
        :connections => Table.new("Connections", "/api/connections", [
          col("Vhost", 12, "vhost") { |c| Fields.text(c, "vhost") },
          col("User", 12, "user") { |c| Fields.text(c, "user") },
          col("State", 10, "state", status: true) { |c| Fields.text(c, "state") },
          col("Chans", 6, "channels", true) { |c| Fields.text(c, "channels") },
          col("Recv/s", 11, "recv_oct_details.rate", true) { |c| Fields.bytes_rate(c, "recv_oct_details", "rate") },
          col("Send/s", 11, "send_oct_details.rate", true) { |c| Fields.bytes_rate(c, "send_oct_details", "rate") },
          col("Client", 20) { |c| Fields.text(c, "client_properties", "connection_name", default: Fields.text(c, "client_properties", "product")) },
          col("Name", 30, "name") { |c| Fields.text(c, "name") },
        ]),
        :channels => Table.new("Channels", "/api/channels", [
          col("Vhost", 12, "vhost") { |c| Fields.text(c, "vhost") },
          col("User", 12, "user") { |c| Fields.text(c, "user") },
          col("State", 10, "state", status: true) { |c| Fields.text(c, "state") },
          col("Unacked", 8, "messages_unacknowledged", true) { |c| Fields.text(c, "messages_unacknowledged") },
          col("Prefetch", 8, "prefetch_count", true) { |c| Fields.text(c, "prefetch_count") },
          col("Cons", 6, "consumer_count", true) { |c| Fields.text(c, "consumer_count") },
          col("Pub/s", 9, "message_stats.publish_details.rate", true) { |c| Fields.rate(c, "message_stats", "publish_details", "rate") },
          col("Name", 30, "name") { |c| Fields.text(c, "name") },
        ]),
        :exchanges => Table.new("Exchanges", "/api/exchanges", [
          col("Vhost", 12, "vhost") { |e| Fields.text(e, "vhost") },
          col("Name", 30, "name") { |e| Fields.text(e, "name").presence || "(default)" },
          col("Type", 14, "type") { |e| Fields.text(e, "type") },
          col("Durable", 7, "durable") { |e| Fields.bool(e, "durable") },
          col("Internal", 8, "internal") { |e| Fields.bool(e, "internal") },
          col("In/s", 9, "message_stats.publish_in_details.rate", true) { |e| Fields.rate(e, "message_stats", "publish_in_details", "rate") },
          col("Out/s", 9, "message_stats.publish_out_details.rate", true) { |e| Fields.rate(e, "message_stats", "publish_out_details", "rate") },
        ]),
        :consumers => Table.new("Consumers", "/api/consumers", [
          col("Vhost", 12, "queue.vhost") { |c| Fields.text(c, "queue", "vhost") },
          col("Queue", 28, "queue.name") { |c| Fields.text(c, "queue", "name") },
          col("Tag", 28, "consumer_tag") { |c| Fields.text(c, "consumer_tag") },
          col("Ack", 4, "ack_required") { |c| Fields.bool(c, "ack_required") },
          col("Prefetch", 8, "prefetch_count", true) { |c| Fields.text(c, "prefetch_count") },
          col("Channel", 30, "channel_details.name") { |c| Fields.text(c, "channel_details", "name") },
        ]),
        :vhosts => Table.new("Vhosts", "/api/vhosts", [
          col("Name", 24, "name") { |v| Fields.text(v, "name") },
          col("Msgs", 10, "messages", true) { |v| Fields.text(v, "messages") },
          col("Ready", 10, "messages_ready", true) { |v| Fields.text(v, "messages_ready") },
          col("Unacked", 10, "messages_unacknowledged", true) { |v| Fields.text(v, "messages_unacknowledged") },
          col("Tracing", 7, "tracing") { |v| Fields.bool(v, "tracing") },
          col("Description", 30) { |v| Fields.text(v, "description") },
        ]),
        :nodes => Table.new("Nodes", "/api/nodes", [
          col("Name", 24) { |n| Fields.text(n, "name") },
          col("Role", 10) { |n| Fields.text(n, "role") },
          col("Uptime", 8) { |n| Fields.dig(n, "uptime") ? Fields.duration(n, "uptime") : "-" },
          col("Memory", 10) { |n| Fields.dig(n, "mem_used") ? Fields.bytes(n, "mem_used") : "-" },
          col("Disk free", 10) { |n| Fields.dig(n, "disk_free") ? Fields.bytes(n, "disk_free") : "-" },
          col("FD", 7) { |n| Fields.text(n, "fd_used") },
          col("Sockets", 7) { |n| Fields.text(n, "sockets_used") },
          col("Lag", 10) { |n| Fields.dig(n, "lag_in_bytes") ? Fields.bytes(n, "lag_in_bytes") : "-" },
        ]),
        :parameters => Table.new("Parameters", "/api/parameters", [
          col("Component", 20, "component") { |p| Fields.text(p, "component") },
          col("Vhost", 12, "vhost") { |p| Fields.text(p, "vhost") },
          col("Name", 24, "name") { |p| Fields.text(p, "name") },
          col("Value", 50) { |p| parameter_summary(p) },
        ]),
        :policies => Table.new("Policies", "/api/policies", [
          col("Vhost", 12, "vhost") { |p| Fields.text(p, "vhost") },
          col("Name", 22, "name") { |p| Fields.text(p, "name") },
          col("Apply to", 10, "apply-to") { |p| Fields.text(p, "apply-to") },
          col("Prio", 5, "priority", true) { |p| Fields.text(p, "priority") },
          col("Pattern", 24, "pattern") { |p| Fields.text(p, "pattern") },
          col("Definition", 40) { |p| Fields.text(p, "definition") },
        ]),
        :shovels => Table.new("Shovels", "/api/shovels", [
          col("Vhost", 12, "vhost") { |s| Fields.text(s, "vhost") },
          col("Name", 24, "name") { |s| Fields.text(s, "name") },
          col("State", 12, "state", status: true) { |s| Fields.text(s, "state") },
          col("Msgs", 9, "message_count", true) { |s| Fields.text(s, "message_count") },
          col("Error", 50) { |s| Fields.redact_uris(Fields.text(s, "error")) },
        ]),
        :federation => Table.new("Federation links", "/api/federation-links", [
          col("Vhost", 12, "vhost") { |l| Fields.text(l, "vhost") },
          col("Upstream", 18, "upstream") { |l| Fields.text(l, "upstream") },
          col("Type", 8, "type") { |l| Fields.text(l, "type") },
          col("Resource", 20, "resource") { |l| Fields.text(l, "resource") },
          col("Status", 10, "status", status: true) { |l| Fields.text(l, "status") },
          col("Error", 22) { |l| Fields.redact_uris(Fields.text(l, "error")) },
          col("URI", 30) { |l| Fields.uri(l, "uri") },
        ]),
        :users => Table.new("Users", "/api/users", [
          col("Name", 24, "name") { |u| Fields.text(u, "name") },
          col("Tags", 28, "tags") { |u| Fields.text(u, "tags") },
          col("Password", 9) { |u| Fields.text(u, "password_hash", default: "").empty? ? "-" : "set" },
          col("Algorithm", 34, "hashing_algorithm") { |u| Fields.text(u, "hashing_algorithm") },
        ]),
      }
    end

    private def parameter_summary(parameter : JSON::Any) : String
      value = Fields.dig(parameter, "value")
      if Fields.dig(value, "src-queue") || Fields.dig(value, "dest-queue")
        "src=#{Fields.text(value, "src-queue")} dest=#{Fields.text(value, "dest-queue")}"
      elsif Fields.dig(value, "uri")
        "uri=#{Fields.uri(value, "uri")}"
      else
        Fields.redact_uris(Fields.text(value))
      end
    end
  end
end

LavinMQCtl.tui_launcher = ->(client : HTTP::Client, reconnect : Proc(HTTP::Client)?, interval : Float64) {
  LavinMQCtl::TUI.new(client, interval, reconnect: reconnect).start
}
