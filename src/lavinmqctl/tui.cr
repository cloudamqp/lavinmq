require "json"
require "http/client"
require "uri"
require "./cli"
require "./tui/screen"
require "./tui/termisu_screen"

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
    record Column, title : String, width : Int32, sort : String?, descending : Bool, value : Proc(JSON::Any, String)

    record Table, title : String, path : String, columns : Array(Column), sort : String? = nil

    # Where the user is in a table page
    class TableState
      property cursor = 0
      property total = 0
      property sort : String?
      property? descending : Bool
      property filter = ""

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
      {"o", "Sort by the next column"},
      {"r", "Reverse the sort order"},
      {"/", "Filter by name, Esc clears the filter"},
      {"p Space", "Pause or resume refreshing"},
      {"?", "Show or hide this help"},
      {"q Ctrl-C", "Quit"},
    }

    BG        = Color.rgb(6, 10, 18)
    HEADER_BG = Color.rgb(9, 18, 32)
    PANEL_BG  = Color.rgb(10, 17, 29)
    ROW_BG    = Color.rgb(13, 22, 36)
    SELECT_BG = Color.rgb(28, 52, 88)
    GRID_FG   = Color.rgb(35, 48, 72)
    TEXT_FG   = Color.rgb(214, 224, 235)
    MUTED_FG  = Color.rgb(128, 145, 166)
    CYAN      = Color.rgb(64, 224, 208)
    BLUE      = Color.rgb(89, 149, 255)
    GREEN     = Color.rgb(82, 230, 139)
    YELLOW    = Color.rgb(245, 208, 90)
    ORANGE    = Color.rgb(255, 151, 82)
    MAGENTA   = Color.rgb(213, 104, 255)
    RED       = Color.rgb(255, 95, 112)
    WHITE     = Color.rgb(238, 244, 252)
    # Braille cells filled from the bottom, a quarter at a time
    GRAPH_FILL = {'⣀', '⣤', '⣶', '⣿'}
    # Braille cells with one row of dots, from the bottom
    GRAPH_LINE = {'⣀', '⠤', '⠒', '⠉'}

    # Waits at least *interval* between refreshes, and long enough that the
    # broker spends at most a tenth of its time answering the TUI, up to 30s
    def self.refresh_delay(interval : Time::Span, fetch_time : Time::Span) : Time::Span
      {interval, {fetch_time * 10, 30.seconds}.min}.max
    end

    # *reconnect* opens a new connection after a timeout, for clients that
    # can't reconnect by themselves, like one on the control socket
    def initialize(@client : HTTP::Client, @interval : Float64 = 1.0, @screen : Screen = TermisuScreen.new, @reconnect : Proc(HTTP::Client)? = nil)
      @running = true
      @closed = false
      @width = 0
      @height = 0
      @page = :overview
      @last_error = nil.as(String?)
      @overview = nil.as(JSON::Any?)
      @items = [] of JSON::Any
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
      end
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
        @nodes = fetch_list("/api/nodes", "nodes")
      else
        fetch_table
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

    private def fetch_json(path : String, label : String) : JSON::Any?
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
    rescue ex : IO::TimeoutError
      # The connection is kept open, so the late response would be read as the
      # answer to the next request
      @client.close
      @closed = true
      record_error("#{label}: #{ex.message}")
      nil
    rescue ex
      record_error("#{label}: #{ex.message || ex.class.name}")
      nil
    end

    # Keeps the first error of a refresh, later ones are often caused by it
    private def record_error(message : String)
      @last_error ||= message
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
      fill_rect(0, 0, @width, 1, bg: HEADER_BG)
      x = print_at(0, 0, " LavinMQ TUI ", BG, CYAN, true)
      x += print_at(x, 0, " #{PAGES.find! { |p| p[:name] == @page }[:label]} ", WHITE, HEADER_BG, true)
      node = Fields.text(@overview, "node", default: "?")
      version = Fields.text(@overview, "lavinmq_version", default: "?")
      print_at(x, 0, " Node: #{node} | v#{version} ", MUTED_FG, HEADER_BG, max_width: @width - x - 24)

      delay = refresh_delay
      status, color = if @paused
                        {" PAUSED ", YELLOW}
                      elsif delay > @interval.seconds
                        {" refresh every %.1fs " % delay.total_seconds, ORANGE}
                      else
                        {"", MUTED_FG}
                      end
      print_at(@width - Text.width(status), 0, status, color, HEADER_BG, true)
    end

    private def draw_footer
      y = @height - 1
      return if y < 1
      fill_rect(0, y, @width, 1, bg: HEADER_BG)
      if input = @input
        title = @tables[@page].title.downcase
        x = print_at(0, y, " Filter #{title} by name: ", YELLOW, HEADER_BG, true)
        x += print_at(x, y, input, WHITE, HEADER_BG)
        print_at(x, y, "▏  Enter to apply, Esc to cancel", MUTED_FG, HEADER_BG)
        return
      end

      nav = PAGES.join(" ") { |p| "[#{p[:key]}]#{p[:nav]}" }
      text = " #{nav}  [?]Help [q]Quit "
      if error = @last_error
        error_text = " #{error} "
        error_width = {Text.width(error_text), @width // 2}.min
        print_fit(0, y, text, @width - error_width, MUTED_FG, HEADER_BG)
        print_fit(@width - error_width, y, error_text, error_width, RED, HEADER_BG, true)
      else
        print_fit(0, y, text, @width, MUTED_FG, HEADER_BG)
      end
    end

    private def draw_help
      width = {64, @width - 4}.min
      height = HELP.size + 4
      rect = Rect.new((@width - width) // 2, {(@height - height) // 2, 1}.max, width, height)
      draw_panel(rect, "Keys", YELLOW)
      HELP.each_with_index do |(keys, text), i|
        y = rect.inner_y + 1 + i
        break if y >= rect.bottom
        print_fit(rect.inner_x + 2, y, keys, 16, CYAN, PANEL_BG, true)
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
      table = @tables[@page]
      rect = Rect.new(1, 2, @width - 2, @height - 4)
      first = (state.cursor // table_rows) * table_rows
      draw_panel(rect, table_title(table, state, first), CYAN)

      x = rect.inner_x + 2
      width = rect.inner_width - 4
      draw_row(rect.inner_y + 1, table_headers(table, state), table.columns, CYAN, PANEL_BG, true, x, width)
      if @items.empty?
        print_at(x, rect.inner_y + 3, state.filter.empty? ? "No data" : "Nothing matches the filter", MUTED_FG, PANEL_BG)
        return
      end

      @items.each_with_index do |item, i|
        y = rect.inner_y + 2 + i
        break if y >= rect.bottom
        selected = first + i == state.cursor
        bg = selected ? SELECT_BG : (i.even? ? PANEL_BG : ROW_BG)
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        values = table.columns.map(&.value.call(item))
        draw_row(y, values, table.columns, selected ? WHITE : TEXT_FG, bg, selected, x, width)
      end
    end

    private def table_title(table : Table, state : TableState, first : Int32) : String
      String.build do |s|
        s << table.title
        s << "  " << first + 1 << "-" << first + @items.size << " of " << state.total unless @items.empty?
        s << "  filter \"" << state.filter << '"' unless state.filter.empty?
      end
    end

    # The sorted column gets an arrow for the sort order
    private def table_headers(table : Table, state : TableState) : Array(String)
      table.columns.map do |column|
        next column.title unless column.sort && column.sort == state.sort
        "#{column.title} #{state.descending? ? '▼' : '▲'}"
      end
    end

    # The last column gets the width that's left
    private def draw_row(y : Int32, values : Array(String), columns : Array(Column), fg : Color, bg : Color, bold : Bool, x : Int32, max_width : Int32)
      used = 0
      columns.each_with_index do |column, i|
        break if used >= max_width
        width = i == columns.size - 1 ? max_width - used : {column.width, max_width - used}.min
        print_fit(x + used, y, values[i], width, fg, bg, bold)
        used += width + 1
      end
    end

    private def draw_overview
      unless overview = @overview
        draw_panel(Rect.new(1, 2, @width - 2, 5), "Error", RED)
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
      draw_panel(rect, "Object totals", CYAN)
      totals = {
        {"Connections", "connections", CYAN}, {"Channels", "channels", BLUE},
        {"Queues", "queues", GREEN}, {"Consumers", "consumers", YELLOW},
        {"Exchanges", "exchanges", MAGENTA}, {"Bindings", "bindings", ORANGE},
      }
      column_width = (rect.inner_width - 4) // 2
      totals.each_with_index do |(label, key, color), i|
        x = rect.inner_x + 2 + (i % 2) * column_width
        y = rect.inner_y + 1 + (i // 2) * 2
        print_at(x, y, label, MUTED_FG, PANEL_BG, max_width: column_width - 2)
        print_at(x, y + 1, Fields.text(overview, "object_totals", key), color, PANEL_BG, true, column_width - 2)
      end
    end

    private def draw_messages_panel(rect : Rect, overview : JSON::Any)
      draw_panel(rect, "Messages", GREEN)
      total = Fields.float(overview, "queue_totals", "messages")
      ready = Fields.float(overview, "queue_totals", "messages_ready")
      unacked = Fields.float(overview, "queue_totals", "messages_unacknowledged")
      x = rect.inner_x + 2
      y = rect.inner_y + 1
      print_at(x, y, "Total", MUTED_FG, PANEL_BG)
      print_at(x + 13, y, Fields.int(overview, "queue_totals", "messages").to_s, WHITE, PANEL_BG, true)
      print_at(x, y + 1, "Publish", MUTED_FG, PANEL_BG)
      print_at(x + 13, y + 1, Fields.rate(overview, "message_stats", "publish_details", "rate") + "/s", CYAN, PANEL_BG, true)
      print_at(x, y + 2, "Deliver", MUTED_FG, PANEL_BG)
      print_at(x + 13, y + 2, Fields.rate(overview, "message_stats", "deliver_get_details", "rate") + "/s", MAGENTA, PANEL_BG, true)
      draw_bar(x, y + 4, rect.inner_width - 4, "Ready", ready, total, GREEN)
      draw_bar(x, y + 6, rect.inner_width - 4, "Unacked", unacked, total, ORANGE)
    end

    private def draw_node_panel(rect : Rect, overview : JSON::Any, node : JSON::Any?)
      draw_panel(rect, "Node resources", BLUE)
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
      draw_bar(x, y + 3, width, "Memory", mem_used, positive_or(Fields.float(node, "mem_limit"), mem_used), CYAN, bytes: true)
      return if y + 4 >= rect.bottom
      disk_total = Fields.float(node, "disk_total")
      disk_free = Fields.float(node, "disk_free")
      if disk_total > 0
        draw_bar(x, y + 4, width, "Disk", disk_total - disk_free, disk_total, YELLOW, bytes: true)
      else
        print_at(x, y + 4, "Disk free #{Fields.bytes(node, "disk_free")}", MUTED_FG, PANEL_BG, max_width: width)
      end
      return if y + 5 >= rect.bottom
      fd_used = Fields.float(node, "fd_used")
      draw_bar(x, y + 5, width, "FD", fd_used, positive_or(Fields.float(node, "fd_total"), fd_used), MAGENTA)
      return if y + 7 >= rect.bottom
      recv = Fields.bytes_rate(overview, "recv_oct_details", "rate")
      send = Fields.bytes_rate(overview, "send_oct_details", "rate")
      print_at(x, y + 7, "Network", MUTED_FG, PANEL_BG)
      print_at(x + 10, y + 7, "in #{recv}  out #{send}", TEXT_FG, PANEL_BG, max_width: width - 10)
      return if y + 8 >= rect.bottom
      followers = Fields.dig(node, "followers").try(&.as_a?) || [] of JSON::Any
      cluster = if followers.empty?
                  "single node"
                else
                  lag = followers.max_of { |f| Fields.int(f, "lag_in_bytes") }
                  "#{followers.size} follower#{"s" if followers.size > 1}, lag #{Fields.human_bytes(lag)}"
                end
      print_at(x, y + 8, "Cluster", MUTED_FG, PANEL_BG)
      print_at(x + 10, y + 8, cluster, TEXT_FG, PANEL_BG, max_width: width - 10)
    end

    private def draw_rate_graph(rect : Rect, overview : JSON::Any)
      publish = Fields.rate(overview, "message_stats", "publish_details", "rate")
      deliver = Fields.rate(overview, "message_stats", "deliver_get_details", "rate")
      draw_panel(rect, "Rate graph  pub #{publish}/s  deliver #{deliver}/s", CYAN)
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      max = draw_graph(graph, @publish_history, CYAN, @deliver_history, MAGENTA)
      print_at(rect.inner_x + 2, rect.bottom - 1, "⣿ publish", CYAN, PANEL_BG)
      print_at(rect.inner_x + 15, rect.bottom - 1, "⠤ deliver", MAGENTA, PANEL_BG)
      draw_graph_scale(rect, "%.1f/s" % max)
    end

    private def draw_queue_graph(rect : Rect, overview : JSON::Any)
      ready = Fields.int(overview, "queue_totals", "messages_ready")
      unacked = Fields.int(overview, "queue_totals", "messages_unacknowledged")
      draw_panel(rect, "Queue depth  ready #{ready}  unacked #{unacked}", GREEN)
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      max = draw_graph(graph, @ready_history, GREEN, @unacked_history, ORANGE)
      print_at(rect.inner_x + 2, rect.bottom - 1, "⣿ ready", GREEN, PANEL_BG)
      print_at(rect.inner_x + 14, rect.bottom - 1, "⠤ unacked", ORANGE, PANEL_BG)
      draw_graph_scale(rect, Fields.to_i64(max).to_s)
    end

    private def draw_hot_queues(rect : Rect)
      draw_panel(rect, "Busiest queues", YELLOW)
      x = rect.inner_x + 2
      width = rect.inner_width - 4
      columns = @tables[:queues].columns.reject(&.title.in?("Vhost", "State"))
      draw_row(rect.inner_y + 1, columns.map(&.title), columns, CYAN, PANEL_BG, true, x, width)
      @items.each_with_index do |queue, i|
        y = rect.inner_y + 2 + i
        break if y >= rect.bottom
        bg = i.even? ? PANEL_BG : ROW_BG
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        draw_row(y, columns.map(&.value.call(queue)), columns, TEXT_FG, bg, false, x, width)
      end
    end

    private def draw_panel(rect : Rect, title : String, color : Color)
      return if rect.width <= 1 || rect.height <= 1

      fill_rect(rect.x, rect.y, rect.width, rect.height, bg: PANEL_BG)
      rect.width.times do |i|
        set_cell(rect.x + i, rect.y, '─', color, PANEL_BG)
        set_cell(rect.x + i, rect.bottom, '─', color, PANEL_BG)
      end
      rect.height.times do |i|
        set_cell(rect.x, rect.y + i, '│', color, PANEL_BG)
        set_cell(rect.right, rect.y + i, '│', color, PANEL_BG)
      end
      set_cell(rect.x, rect.y, '╭', color, PANEL_BG)
      set_cell(rect.right, rect.y, '╮', color, PANEL_BG)
      set_cell(rect.x, rect.bottom, '╰', color, PANEL_BG)
      set_cell(rect.right, rect.bottom, '╯', color, PANEL_BG)
      x = rect.x + 2
      x += print_at(x, rect.y, " ", color, PANEL_BG)
      x += print_at(x, rect.y, title, color, PANEL_BG, true, rect.width - 6)
      print_at(x, rect.y, " ", color, PANEL_BG)
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

      levels = rect.height * GRAPH_FILL.size
      rect.width.times do |i|
        x = rect.right - i
        if value = area[-1 - i]?
          full, partial = (value / max * levels).ceil.clamp(0, levels).to_i.divmod(GRAPH_FILL.size)
          full.times { |row| set_cell(x, rect.bottom - row, GRAPH_FILL[-1], area_color, PANEL_BG) }
          set_cell(x, rect.bottom - full, GRAPH_FILL[partial - 1], area_color, PANEL_BG) if partial > 0
        end
        if value = line[-1 - i]?
          # Zero is drawn on the bottom row, the line never disappears
          level = (value / max * levels).ceil.clamp(1, levels).to_i
          row, dot = (level - 1).divmod(GRAPH_LINE.size)
          set_cell(x, rect.bottom - row, GRAPH_LINE[dot], line_color, PANEL_BG)
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

    private def draw_graph_scale(panel : Rect, max : String)
      text = "max #{max}"
      print_at(panel.right - 2 - text.size, panel.bottom - 1, text, MUTED_FG, PANEL_BG)
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

    private def col(title : String, width : Int32, sort : String? = nil, descending = false, &value : JSON::Any -> String) : Column
      Column.new(title, width, sort, descending, value)
    end

    private def tables : Hash(Symbol, Table)
      {
        :queues => Table.new("Queues", "/api/queues", [
          col("Vhost", 12, "vhost") { |q| Fields.text(q, "vhost") },
          col("Name", 30, "name") { |q| Fields.text(q, "name") },
          col("State", 9, "state") { |q| Fields.text(q, "state") },
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
          col("State", 8, "state") { |c| Fields.text(c, "state") },
          col("Chans", 6, "channels", true) { |c| Fields.text(c, "channels") },
          col("Recv/s", 11, "recv_oct_details.rate", true) { |c| Fields.bytes_rate(c, "recv_oct_details", "rate") },
          col("Send/s", 11, "send_oct_details.rate", true) { |c| Fields.bytes_rate(c, "send_oct_details", "rate") },
          col("Client", 20) { |c| Fields.text(c, "client_properties", "connection_name", default: Fields.text(c, "client_properties", "product")) },
          col("Name", 30, "name") { |c| Fields.text(c, "name") },
        ]),
        :channels => Table.new("Channels", "/api/channels", [
          col("Vhost", 12, "vhost") { |c| Fields.text(c, "vhost") },
          col("User", 12, "user") { |c| Fields.text(c, "user") },
          col("State", 8, "state") { |c| Fields.text(c, "state") },
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
          col("State", 12, "state") { |s| Fields.text(s, "state") },
          col("Msgs", 9, "message_count", true) { |s| Fields.text(s, "message_count") },
          col("Error", 50) { |s| Fields.redact_uris(Fields.text(s, "error")) },
        ]),
        :federation => Table.new("Federation links", "/api/federation-links", [
          col("Vhost", 12, "vhost") { |l| Fields.text(l, "vhost") },
          col("Upstream", 18, "upstream") { |l| Fields.text(l, "upstream") },
          col("Type", 8, "type") { |l| Fields.text(l, "type") },
          col("Resource", 20, "resource") { |l| Fields.text(l, "resource") },
          col("Status", 8, "status") { |l| Fields.text(l, "status") },
          col("Error", 24) { |l| Fields.redact_uris(Fields.text(l, "error")) },
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
