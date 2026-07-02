require "json"
require "http/client"
require "termisu"

class LavinMQCtl
  class TUI
    abstract class Screen
      abstract def size : {Int32, Int32}
      abstract def poll_event(timeout_ms : Int32) : Termisu::Event::Any?
      abstract def clear : Nil
      abstract def set_cell(
        x : Int32,
        y : Int32,
        char : Char,
        fg : Termisu::Color,
        bg : Termisu::Color,
        attr : Termisu::Attribute,
      ) : Nil
      abstract def render : Nil
      abstract def sync : Nil
      abstract def close : Nil
    end

    class TermisuScreen < Screen
      def initialize
        @termisu = Termisu.new
      end

      def size : {Int32, Int32}
        @termisu.size
      end

      def poll_event(timeout_ms : Int32) : Termisu::Event::Any?
        @termisu.poll_event(timeout_ms)
      end

      def clear : Nil
        @termisu.clear
      end

      def render : Nil
        @termisu.render
      end

      def sync : Nil
        @termisu.sync
      end

      def close : Nil
        @termisu.close
      end

      def set_cell(
        x : Int32,
        y : Int32,
        char : Char,
        fg : Termisu::Color,
        bg : Termisu::Color,
        attr : Termisu::Attribute,
      ) : Nil
        @termisu.set_cell(x, y, char, fg, bg, attr)
      end
    end

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

    BG          = Termisu::Color.rgb(6, 10, 18)
    PANEL_BG    = Termisu::Color.rgb(10, 17, 29)
    ROW_BG      = Termisu::Color.rgb(13, 22, 36)
    BAR_BG      = Termisu::Color.rgb(20, 31, 49)
    GRID_FG     = Termisu::Color.rgb(35, 48, 72)
    TEXT_FG     = Termisu::Color.rgb(214, 224, 235)
    MUTED_FG    = Termisu::Color.rgb(128, 145, 166)
    DIM_FG      = Termisu::Color.rgb(84, 101, 124)
    CYAN        = Termisu::Color.rgb(64, 224, 208)
    BLUE        = Termisu::Color.rgb(89, 149, 255)
    GREEN       = Termisu::Color.rgb(82, 230, 139)
    YELLOW      = Termisu::Color.rgb(245, 208, 90)
    ORANGE      = Termisu::Color.rgb(255, 151, 82)
    MAGENTA     = Termisu::Color.rgb(213, 104, 255)
    RED         = Termisu::Color.rgb(255, 95, 112)
    WHITE       = Termisu::Color.rgb(238, 244, 252)
    BRAILLE_BAR = {'⠁', '⠃', '⠇', '⡇'}

    def initialize(@client : HTTP::Client, @interval : Float64 = 1.0, @screen : Screen = TermisuScreen.new)
      @running = true
      @width = 0
      @height = 0
      @page = :overview
      @last_error = nil.as(String?)
      @publish_history = [] of Float64
      @deliver_history = [] of Float64
      @ready_history = [] of Float64
      @unacked_history = [] of Float64
    end

    def start
      @width, @height = @screen.size
      render

      poll_ms = (@interval * 1000).to_i

      loop do
        if event = @screen.poll_event(poll_ms)
          case event
          when Termisu::Event::Key
            handle_key(event)
          when Termisu::Event::Resize
            @width = event.width
            @height = event.height
            @screen.sync
            render
          end
        else
          render
        end

        break unless @running
      end
    ensure
      @screen.close
    end

    def render_once
      @width, @height = @screen.size
      render
    end

    private def handle_key(event : Termisu::Event::Key)
      if event.ctrl_c? || event.char == 'q'
        @running = false
        return
      end

      if page = PAGES.find { |p| p[:key] == event.char }
        @page = page[:name]
        render
      end
    end

    private def render
      @screen.clear
      @width, @height = @screen.size
      @last_error = nil
      paint_background

      overview = fetch_overview
      render_header(overview)

      if @page == :overview
        render_overview_page(overview)
      else
        render_collection_page
      end

      render_footer
      @screen.render
    end

    private def render_collection_page
      case @page
      when :shovels, :federation, :users then render_admin_page
      else                                    render_broker_page
      end
    end

    private def render_broker_page
      case @page
      when :queues      then render_queues_page(fetch_items("/api/queues", "queues", "page=1&page_size=20&sort=messages&sort_reverse=true"))
      when :connections then render_connections_page(fetch_items("/api/connections", "connections"))
      when :channels    then render_channels_page(fetch_items("/api/channels", "channels"))
      when :exchanges   then render_exchanges_page(fetch_items("/api/exchanges", "exchanges"))
      when :consumers   then render_consumers_page(fetch_items("/api/consumers", "consumers"))
      when :vhosts      then render_vhosts_page(fetch_items("/api/vhosts", "vhosts"))
      when :nodes       then render_nodes_page(fetch_items("/api/nodes", "nodes"))
      when :parameters  then render_parameters_page(fetch_items("/api/parameters", "parameters"))
      when :policies    then render_policies_page(fetch_items("/api/policies", "policies"))
      end
    end

    private def render_admin_page
      case @page
      when :shovels    then render_shovels_page(fetch_items("/api/shovels", "shovels"))
      when :federation then render_federation_page(fetch_items("/api/federation-links", "federation"))
      when :users      then render_users_page(fetch_items("/api/users", "users"))
      end
    end

    private def fetch_overview
      fetch_json("/api/overview", "overview")
    end

    private def fetch_items(path : String, label : String, query = "page=1&page_size=20") : Array(JSON::Any)
      separator = path.includes?('?') ? '&' : '?'
      data = fetch_json("#{path}#{separator}#{query}", label)
      return [] of JSON::Any unless data

      if items = child(data, "items").try(&.as_a?)
        items
      elsif items = data.as_a?
        items
      else
        record_error("#{label}: missing items")
        [] of JSON::Any
      end
    end

    private def fetch_json(path : String, label : String) : JSON::Any?
      response = @client.get(path)
      unless response.status_code == 200
        record_error("#{label}: HTTP #{response.status_code} #{response.status}")
        return nil
      end
      JSON.parse(response.body)
    rescue ex : JSON::ParseException
      record_error("#{label}: invalid JSON (#{ex.message})")
      nil
    rescue ex
      record_error("#{label}: #{ex.message || ex.class.name}")
      nil
    end

    private def record_error(message : String)
      @last_error = message
    end

    private def render_header(overview : JSON::Any?)
      version = json_text(child(overview, "lavinmq_version"), "?")
      node = json_text(child(overview, "node"), "Unknown")
      page = page_label(@page)

      header_bg = Termisu::Color.rgb(9, 18, 32)
      header_text = " LavinMQ TUI "
      page_text = " #{page} "
      info_text = " Node: #{node} | v#{version} "

      @width.times do |i|
        set_cell(i, 0, ' ', TEXT_FG, header_bg)
      end
      print_at(0, 0, header_text, BG, CYAN, Termisu::Attribute::Bold)
      print_at(header_text.size, 0, page_text, WHITE, header_bg, Termisu::Attribute::Bold)
      print_at(header_text.size + page_text.size, 0, info_text, MUTED_FG, header_bg)
    end

    private def render_footer
      y = @height - 1
      return if y < 0

      footer_bg = Termisu::Color.rgb(9, 18, 32)
      footer_fg = MUTED_FG
      nav = PAGES.map { |p| "[#{p[:key]}]#{p[:nav]}" }.join(" ")
      footer_text = " #{nav} [q] Quit "

      @width.times do |i|
        set_cell(i, y, ' ', footer_fg, footer_bg)
      end
      if error = @last_error
        error_text = "| #{error} "
        nav_width = {@width - error_text.size, 0}.max
        print_at(0, y, fit(footer_text, nav_width), footer_fg, footer_bg)
        print_at(nav_width, y, error_text, RED, footer_bg, Termisu::Attribute::Bold)
      else
        print_at(0, y, footer_text, footer_fg, footer_bg)
      end
    end

    private def render_overview_page(overview : JSON::Any?)
      unless overview
        render_empty("Overview unavailable")
        return
      end

      update_histories(overview)

      if @width < 100 || @height < 28
        render_compact_overview(overview)
        return
      end

      left_width = {@width // 3, 42}.min
      left_width = {left_width, 34}.max
      right_width = @width - left_width - 3

      totals_rect = Rect.new(1, 2, left_width, 9)
      messages_rect = Rect.new(1, 11, left_width, 10)
      node_rect = Rect.new(1, 21, left_width, @height - 23)
      rate_rect = Rect.new(left_width + 2, 2, right_width, 10)
      queue_graph_rect = Rect.new(left_width + 2, 13, right_width, 10)
      hot_rect = Rect.new(left_width + 2, 24, right_width, @height - 26)

      queues = fetch_items("/api/queues", "queues", "page=1&page_size=8&sort=messages&sort_reverse=true")
      nodes = fetch_items("/api/nodes", "nodes")

      render_totals_panel(totals_rect, overview)
      render_messages_panel(messages_rect, overview)
      render_node_panel(node_rect, nodes.first?)
      render_rate_graph(rate_rect, overview)
      render_queue_graph(queue_graph_rect, overview)
      render_hot_queues(hot_rect, queues)
    end

    private def render_compact_overview(overview : JSON::Any)
      render_totals_panel(Rect.new(1, 2, @width - 2, 8), overview)
      render_messages_panel(Rect.new(1, 10, @width - 2, 9), overview)
      graph_height = {@height - 21, 5}.max
      render_rate_graph(Rect.new(1, 19, @width - 2, graph_height), overview)
    end

    private def render_totals_panel(rect : Rect, overview : JSON::Any)
      draw_panel(rect, "Object totals", CYAN)
      totals = child(overview, "object_totals")
      rows = [
        {"Connections", json_text(child(totals, "connections")), CYAN},
        {"Channels", json_text(child(totals, "channels")), BLUE},
        {"Queues", json_text(child(totals, "queues")), GREEN},
        {"Consumers", json_text(child(totals, "consumers")), YELLOW},
        {"Exchanges", json_text(child(totals, "exchanges")), MAGENTA},
        {"Bindings", json_text(child(totals, "bindings")), ORANGE},
      ]

      y = rect.inner_y + 1
      rows.each_slice(2).with_index do |pair, row|
        x = rect.inner_x + 2
        pair.each do |label, value, color|
          metric_width = (rect.inner_width - 4) // 2
          print_at(x, y + row * 2, label, MUTED_FG, PANEL_BG)
          print_at(x, y + row * 2 + 1, fit(value, metric_width - 2), color, PANEL_BG, Termisu::Attribute::Bold)
          x += metric_width
        end
      end
    end

    private def render_messages_panel(rect : Rect, overview : JSON::Any)
      draw_panel(rect, "Messages", GREEN)
      totals = child(overview, "queue_totals")
      total = json_float(child(totals, "messages"))
      ready = json_float(child(totals, "messages_ready"))
      unacked = json_float(child(totals, "messages_unacknowledged"))
      publish = metric_rate(overview, "publish_details")
      deliver = metric_rate(overview, "deliver_details")

      y = rect.inner_y + 1
      print_at(rect.inner_x + 2, y, "Total", MUTED_FG, PANEL_BG)
      print_at(rect.inner_x + 15, y, total.to_i64.to_s, WHITE, PANEL_BG, Termisu::Attribute::Bold)
      print_at(rect.inner_x + 2, y + 1, "Publish", MUTED_FG, PANEL_BG)
      print_at(rect.inner_x + 15, y + 1, "%.1f/s" % publish, CYAN, PANEL_BG, Termisu::Attribute::Bold)
      print_at(rect.inner_x + 2, y + 2, "Deliver", MUTED_FG, PANEL_BG)
      print_at(rect.inner_x + 15, y + 2, "%.1f/s" % deliver, MAGENTA, PANEL_BG, Termisu::Attribute::Bold)

      draw_bar(rect.inner_x + 2, y + 4, rect.inner_width - 4, "Ready", ready, total, GREEN)
      draw_bar(rect.inner_x + 2, y + 6, rect.inner_width - 4, "Unacked", unacked, total, ORANGE)
    end

    private def render_node_panel(rect : Rect, node : JSON::Any?)
      draw_panel(rect, "Node resources", BLUE)
      unless node
        print_at(rect.inner_x + 2, rect.inner_y + 1, "No node data", MUTED_FG, PANEL_BG)
        return
      end

      mem_used = json_float(child(node, "mem_used"))
      mem_limit = json_float(child(node, "mem_limit"))
      disk_free = json_float(child(node, "disk_free"))
      disk_total = json_float(child(node, "disk_total"))
      fd_used = json_float(child(node, "fd_used"))
      fd_total = json_float(child(node, "fd_total"))
      sockets = json_text(child(node, "sockets_used"))
      uptime = human_duration(json_float(child(node, "uptime")).to_i64)

      y = rect.inner_y + 1
      print_at(rect.inner_x + 2, y, fit(json_text(child(node, "name")), rect.inner_width - 4), WHITE, PANEL_BG, Termisu::Attribute::Bold)
      print_at(rect.inner_x + 2, y + 1, "Uptime #{uptime}", MUTED_FG, PANEL_BG)
      print_at(rect.inner_x + 20, y + 1, "Sockets #{sockets}", MUTED_FG, PANEL_BG)

      draw_bar(rect.inner_x + 2, y + 3, rect.inner_width - 4, "Memory", mem_used, positive_or_self(mem_limit, mem_used), CYAN)
      if disk_total > 0.0
        draw_bar(rect.inner_x + 2, y + 5, rect.inner_width - 4, "Disk", disk_total - disk_free, disk_total, YELLOW)
      else
        print_at(rect.inner_x + 2, y + 5, "Disk free #{human_bytes(disk_free.to_i64)}", MUTED_FG, PANEL_BG)
      end
      draw_bar(rect.inner_x + 2, y + 7, rect.inner_width - 4, "FD", fd_used, positive_or_self(fd_total, fd_used), MAGENTA) if rect.inner_height >= 9
    end

    private def render_rate_graph(rect : Rect, overview : JSON::Any)
      publish = metric_rate(overview, "publish_details")
      deliver = metric_rate(overview, "deliver_details")
      draw_panel(rect, "Rate graph  pub %.1f/s  deliver %.1f/s" % {publish, deliver}, CYAN)
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      draw_dot_graph(graph, @deliver_history, MAGENTA)
      draw_dot_graph(graph, @publish_history, CYAN)
      print_at(rect.inner_x + 2, rect.bottom - 1, "⡇ publish", CYAN, PANEL_BG)
      print_at(rect.inner_x + 15, rect.bottom - 1, "⡇ deliver", MAGENTA, PANEL_BG)
    end

    private def render_queue_graph(rect : Rect, overview : JSON::Any)
      totals = child(overview, "queue_totals")
      ready = json_float(child(totals, "messages_ready"))
      unacked = json_float(child(totals, "messages_unacknowledged"))
      draw_panel(rect, "Queue depth  ready #{ready.to_i64}  unacked #{unacked.to_i64}", GREEN)
      graph = Rect.new(rect.inner_x + 2, rect.inner_y + 1, rect.inner_width - 4, rect.inner_height - 3)
      draw_dot_graph(graph, @ready_history, GREEN)
      draw_dot_graph(graph, @unacked_history, ORANGE)
      print_at(rect.inner_x + 2, rect.bottom - 1, "⡇ ready", GREEN, PANEL_BG)
      print_at(rect.inner_x + 14, rect.bottom - 1, "⡇ unacked", ORANGE, PANEL_BG)
    end

    private def render_hot_queues(rect : Rect, queues : Array(JSON::Any))
      draw_panel(rect, "Hottest queues", YELLOW)
      headers = ["Name", "Msgs", "Ready", "Unacked", "Cons", "Pub/s"]
      widths = [32, 8, 8, 8, 6, 8]
      y = rect.inner_y + 1
      render_row(y, headers, widths, CYAN, Termisu::Attribute::Bold, PANEL_BG, rect.inner_x + 2, rect.inner_width - 4)
      y += 1

      queues.each_with_index do |queue, i|
        break if y >= rect.bottom
        bg = i.even? ? PANEL_BG : ROW_BG
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        row = [
          json_text(child(queue, "name")),
          json_text(child(queue, "messages")),
          json_text(child(queue, "messages_ready")),
          json_text(child(queue, "messages_unacknowledged")),
          json_text(child(queue, "consumers")),
          "%.1f" % json_float(child(child(child(queue, "message_stats"), "publish_details"), "rate")),
        ]
        render_row(y, row, widths, TEXT_FG, Termisu::Attribute::None, bg, rect.inner_x + 2, rect.inner_width - 4)
        y += 1
      end
    end

    private def render_queues_page(queues : Array(JSON::Any))
      render_table(
        "Queues by message count",
        ["Vhost", "Name", "State", "Msgs", "Ready", "Unacked", "Cons", "Pub/s"],
        [10, 28, 9, 8, 8, 8, 6, 8],
        queues.map do |q|
          [
            json_text(child(q, "vhost")),
            json_text(child(q, "name")),
            json_text(child(q, "state")),
            json_text(child(q, "messages")),
            json_text(child(q, "messages_ready")),
            json_text(child(q, "messages_unacknowledged")),
            json_text(child(q, "consumers")),
            "%.1f" % json_float(child(child(child(q, "message_stats"), "publish_details"), "rate")),
          ]
        end
      )
    end

    private def render_connections_page(connections : Array(JSON::Any))
      render_table(
        "Connections",
        ["Vhost", "User", "State", "Channels", "Recv", "Send", "Name"],
        [10, 12, 10, 8, 10, 10, 40],
        connections.map do |conn|
          [
            json_text(child(conn, "vhost")),
            json_text(child(conn, "user")),
            json_text(child(conn, "state")),
            json_text(child(conn, "channels")),
            json_text(child(conn, "recv_oct")),
            json_text(child(conn, "send_oct")),
            json_text(child(conn, "name")),
          ]
        end
      )
    end

    private def render_channels_page(channels : Array(JSON::Any))
      render_table(
        "Channels",
        ["Vhost", "User", "State", "No", "Unacked", "Prefetch", "Consumer", "Name"],
        [10, 12, 10, 5, 8, 8, 8, 36],
        channels.map do |ch|
          [
            json_text(child(ch, "vhost")),
            json_text(child(ch, "user")),
            json_text(child(ch, "state")),
            json_text(child(ch, "number")),
            json_text(child(ch, "messages_unacknowledged")),
            json_text(child(ch, "prefetch_count")),
            json_text(child(ch, "consumer_count")),
            json_text(child(ch, "name")),
          ]
        end
      )
    end

    private def render_exchanges_page(exchanges : Array(JSON::Any))
      render_table(
        "Exchanges",
        ["Vhost", "Name", "Type", "D", "I", "In/s", "Out/s"],
        [10, 32, 14, 3, 3, 8, 8],
        exchanges.map do |ex|
          [
            json_text(child(ex, "vhost")),
            json_text(child(ex, "name"), "(default)"),
            json_text(child(ex, "type")),
            bool_text(child(ex, "durable")),
            bool_text(child(ex, "internal")),
            "%.1f" % json_float(child(child(child(ex, "message_stats"), "publish_in_details"), "rate")),
            "%.1f" % json_float(child(child(child(ex, "message_stats"), "publish_out_details"), "rate")),
          ]
        end
      )
    end

    private def render_consumers_page(consumers : Array(JSON::Any))
      render_table(
        "Consumers",
        ["Vhost", "Queue", "Ack", "Prefetch", "Tag", "Channel"],
        [10, 28, 5, 8, 26, 38],
        consumers.map do |consumer|
          queue = child(consumer, "queue")
          channel = child(consumer, "channel_details")
          [
            json_text(child(queue, "vhost")),
            json_text(child(queue, "name")),
            bool_text(child(consumer, "ack_required")),
            json_text(child(consumer, "prefetch_count")),
            json_text(child(consumer, "consumer_tag")),
            json_text(child(channel, "name")),
          ]
        end
      )
    end

    private def render_vhosts_page(vhosts : Array(JSON::Any))
      render_table(
        "Virtual hosts",
        ["Name", "Messages", "Ready", "Unacked", "Recv", "Send", "Tracing"],
        [24, 10, 10, 10, 10, 10, 8],
        vhosts.map do |vhost|
          [
            json_text(child(vhost, "name")),
            json_text(child(vhost, "messages")),
            json_text(child(vhost, "messages_ready")),
            json_text(child(vhost, "messages_unacknowledged")),
            json_text(child(vhost, "recv_oct")),
            json_text(child(vhost, "send_oct")),
            bool_text(child(vhost, "tracing")),
          ]
        end
      )
    end

    private def render_nodes_page(nodes : Array(JSON::Any))
      render_table(
        "Nodes",
        ["Name", "Uptime", "Mem", "Disk free", "FD used", "Sockets", "RunQ"],
        [22, 10, 10, 12, 8, 8, 6],
        nodes.map do |node|
          [
            json_text(child(node, "name")),
            human_duration(json_float(child(node, "uptime")).to_i64),
            human_bytes(json_float(child(node, "mem_used")).to_i64),
            human_bytes(json_float(child(node, "disk_free")).to_i64),
            json_text(child(node, "fd_used")),
            json_text(child(node, "sockets_used")),
            json_text(child(node, "run_queue")),
          ]
        end
      )
    end

    private def render_parameters_page(parameters : Array(JSON::Any))
      render_table(
        "Parameters",
        ["Component", "Vhost", "Name", "Value"],
        [22, 12, 24, 56],
        parameters.map do |parameter|
          value = child(parameter, "value")
          [
            json_text(child(parameter, "component")),
            json_text(child(parameter, "vhost")),
            json_text(child(parameter, "name")),
            parameter_summary(value),
          ]
        end
      )
    end

    private def render_policies_page(policies : Array(JSON::Any))
      render_table(
        "Policies",
        ["Vhost", "Name", "Apply", "Pri", "Pattern", "Definition"],
        [12, 22, 12, 5, 24, 42],
        policies.map do |policy|
          [
            json_text(child(policy, "vhost")),
            json_text(child(policy, "name")),
            json_text(child(policy, "apply-to")),
            json_text(child(policy, "priority")),
            json_text(child(policy, "pattern")),
            compact_json(child(policy, "definition")),
          ]
        end
      )
    end

    private def render_shovels_page(shovels : Array(JSON::Any))
      render_table(
        "Shovels",
        ["Vhost", "Name", "State", "Msgs", "Error"],
        [12, 24, 12, 8, 60],
        shovels.map do |shovel|
          [
            json_text(child(shovel, "vhost")),
            json_text(child(shovel, "name")),
            json_text(child(shovel, "state")),
            json_text(child(shovel, "message_count")),
            json_text(child(shovel, "error")),
          ]
        end
      )
    end

    private def render_federation_page(links : Array(JSON::Any))
      render_table(
        "Federation",
        ["Vhost", "Name", "Type", "Resource", "URI", "Timestamp"],
        [12, 22, 10, 24, 42, 18],
        links.map do |link|
          [
            json_text(child(link, "vhost")),
            json_text(child(link, "name")),
            json_text(child(link, "type")),
            json_text(child(link, "resource")),
            json_text(child(link, "uri")),
            json_text(child(link, "timestamp")),
          ]
        end
      )
    end

    private def render_users_page(users : Array(JSON::Any))
      render_table(
        "Users",
        ["Name", "Tags", "Password", "Algorithm"],
        [24, 28, 9, 34],
        users.map do |user|
          hash = json_text(child(user, "password_hash"))
          [
            json_text(child(user, "name")),
            json_text(child(user, "tags")),
            hash == "-" ? "-" : "yes",
            json_text(child(user, "hashing_algorithm")),
          ]
        end
      )
    end

    private def render_table(title : String, headers : Array(String), widths : Array(Int32), rows : Array(Array(String)))
      rect = Rect.new(1, 2, @width - 2, @height - 4)
      draw_panel(rect, title, CYAN)
      y = rect.inner_y + 1
      render_row(y, headers, widths, CYAN, Termisu::Attribute::Bold, PANEL_BG, rect.inner_x + 2, rect.inner_width - 4)
      y += 1

      if rows.empty?
        print_at(rect.inner_x + 2, y + 1, "No data", MUTED_FG, PANEL_BG)
        return
      end

      rows.each_with_index do |row, i|
        break if y >= rect.bottom
        bg = i.even? ? PANEL_BG : ROW_BG
        fill_rect(rect.inner_x + 1, y, rect.inner_width - 2, 1, bg: bg)
        render_row(y, row, widths, TEXT_FG, Termisu::Attribute::None, bg, rect.inner_x + 2, rect.inner_width - 4)
        y += 1
      end
    end

    private def render_row(
      y : Int32,
      values : Array(String),
      widths : Array(Int32),
      fg = TEXT_FG,
      attr = Termisu::Attribute::None,
      bg = PANEL_BG,
      x = 2,
      max_width = @width - 2,
    )
      start_x = x
      values.each_with_index do |value, i|
        width = widths[i]? || 10
        break if x - start_x >= max_width
        width = {width, max_width - (x - start_x)}.min
        print_at(x, y, fit(value, width), fg, bg, attr)
        x += width + 1
        break if x >= @width
      end
    end

    private def render_section(title : String, y : Int32) : Int32
      print_at(2, y, title, YELLOW, BG, Termisu::Attribute::Bold)
      y + 2
    end

    private def render_kv_rows(y : Int32, rows : Array(Tuple(String, String)))
      x = 4
      rows.each_with_index do |(label, value), i|
        col = i % 3
        row = i // 3
        print_at(x + (col * 28), y + row, "#{fit(label + ":", 14)} #{fit(value, 10)}")
      end
    end

    private def render_empty(message : String)
      draw_panel(Rect.new(1, 2, @width - 2, 5), "Error", RED)
      print_at(3, 4, message, RED, PANEL_BG, Termisu::Attribute::Bold)
    end

    private def paint_background
      fill_rect(0, 0, @width, @height, bg: BG)
    end

    private def draw_panel(rect : Rect, title : String, color = CYAN)
      return if rect.width <= 1 || rect.height <= 1

      fill_rect(rect.x, rect.y, rect.width, rect.height, bg: PANEL_BG)
      draw_hline(rect.x + 1, rect.y, rect.width - 2, color)
      draw_hline(rect.x + 1, rect.bottom, rect.width - 2, color)
      draw_vline(rect.x, rect.y + 1, rect.height - 2, color)
      draw_vline(rect.right, rect.y + 1, rect.height - 2, color)
      set_cell(rect.x, rect.y, '╭', color, PANEL_BG)
      set_cell(rect.right, rect.y, '╮', color, PANEL_BG)
      set_cell(rect.x, rect.bottom, '╰', color, PANEL_BG)
      set_cell(rect.right, rect.bottom, '╯', color, PANEL_BG)
      print_at(rect.x + 2, rect.y, fit(" #{title} ", rect.width - 4), color, PANEL_BG, Termisu::Attribute::Bold)
    end

    private def draw_hline(x : Int32, y : Int32, width : Int32, color)
      width.times do |i|
        set_cell(x + i, y, '─', color, PANEL_BG)
      end
    end

    private def draw_vline(x : Int32, y : Int32, height : Int32, color)
      height.times do |i|
        set_cell(x, y + i, '│', color, PANEL_BG)
      end
    end

    private def draw_bar(x : Int32, y : Int32, width : Int32, label : String, value : Float64, max : Float64, color)
      return if width <= 0

      label_width = {label.size + 1, 10}.max
      value_text = value >= 1024.0 ? human_bytes(value.to_i64) : value.to_i64.to_s
      value_width = {value_text.size + 1, 8}.max
      bar_width = width - label_width - value_width
      return if bar_width <= 0

      filled = max <= 0.0 ? 0 : ((value / max) * bar_width).round.to_i
      filled = bounded(filled, 0, bar_width)
      print_at(x, y, fit(label, label_width), MUTED_FG, PANEL_BG)
      bar_width.times do |i|
        if i < filled
          set_cell(x + label_width + i, y, '━', color, PANEL_BG, Termisu::Attribute::Bold)
        else
          set_cell(x + label_width + i, y, '·', GRID_FG, PANEL_BG)
        end
      end
      print_at(x + label_width + bar_width + 1, y, fit(value_text, value_width - 1), color, PANEL_BG, Termisu::Attribute::Bold)
    end

    private def draw_dot_graph(rect : Rect, values : Array(Float64), color)
      return if rect.width <= 0 || rect.height <= 0

      draw_graph_grid(rect)
      series = values.last(rect.width)
      return if series.empty?

      min = series.min
      max = series.max
      min = 0.0 if min > 0.0
      span = max - min
      span = 1.0 if span <= 0.0
      levels = rect.height * BRAILLE_BAR.size - 1
      x_offset = rect.width - series.size

      series.each_with_index do |value, i|
        level = (((value - min) / span) * levels).round.to_i
        level = bounded(level, 0, levels)
        row = level // BRAILLE_BAR.size
        dot = level % BRAILLE_BAR.size
        set_cell(rect.x + x_offset + i, rect.bottom - row, BRAILLE_BAR[dot], color, PANEL_BG, Termisu::Attribute::Bold)
      end
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

    private def fill_rect(
      x : Int32,
      y : Int32,
      width : Int32,
      height : Int32,
      char = ' ',
      fg = TEXT_FG,
      bg = PANEL_BG,
      attr = Termisu::Attribute::None,
    )
      height.times do |dy|
        width.times do |dx|
          set_cell(x + dx, y + dy, char, fg, bg, attr)
        end
      end
    end

    private def set_cell(
      x : Int32,
      y : Int32,
      char : Char,
      fg = TEXT_FG,
      bg = BG,
      attr = Termisu::Attribute::None,
    )
      return if x < 0 || x >= @width || y < 0 || y >= @height

      @screen.set_cell(x, y, char, fg, bg, attr)
    end

    private def print_at(x : Int32, y : Int32, text, fg = Termisu::Color.default, bg = Termisu::Color.default, attr = Termisu::Attribute::None)
      return if y < 0 || y >= @height || x >= @width

      text.to_s.each_char_with_index do |char, i|
        next if x + i < 0
        break if x + i >= @width
        @screen.set_cell(x + i, y, char, fg, bg, attr)
      end
    end

    private def fit(value : String, width : Int32) : String
      return "" if width <= 0
      return value.ljust(width) if value.size <= width
      return value[0, width] if width < 3

      "#{value[0, width - 2]}.."
    end

    private def update_histories(overview : JSON::Any)
      stats = child(overview, "message_stats")
      totals = child(overview, "queue_totals")
      update_history(@publish_history, metric_rate(overview, "publish_details"), json_float_array(child(child(stats, "publish_details"), "log")))
      update_history(@deliver_history, metric_rate(overview, "deliver_details"), json_float_array(child(child(stats, "deliver_details"), "log")))
      update_history(@ready_history, json_float(child(totals, "messages_ready")), json_float_array(child(totals, "messages_ready_log")))
      update_history(@unacked_history, json_float(child(totals, "messages_unacknowledged")), json_float_array(child(totals, "messages_unacknowledged_log")))
    end

    private def update_history(history : Array(Float64), current : Float64, log : Array(Float64))
      if log.empty?
        history << current
      else
        history.clear
        history.concat(log)
        history << current if history.empty? || history[-1] != current
      end
      while history.size > 240
        history.shift
      end
    end

    private def metric_rate(overview : JSON::Any, details_key : String) : Float64
      json_float(child(child(child(overview, "message_stats"), details_key), "rate"))
    end

    private def positive_or_self(max : Float64, value : Float64) : Float64
      max > 0.0 ? max : value
    end

    private def bounded(value : Int32, min : Int32, max : Int32) : Int32
      return min if value < min
      return max if value > max
      value
    end

    private def page_label(name : Symbol) : String
      PAGES.find { |p| p[:name] == name }.try(&.[:label]) || name.to_s
    end

    private def child(value : JSON::Any?, key : String) : JSON::Any?
      return unless value

      if object = value.as_h?
        object[key]?
      end
    end

    private def json_text(value : JSON::Any?, default = "-") : String
      return default unless value

      raw = value.raw
      raw.nil? ? default : raw.to_s
    end

    private def json_float(value : JSON::Any?) : Float64
      return 0.0 unless value

      value.as_f? || value.as_i?.try(&.to_f) || 0.0
    end

    private def json_float_array(value : JSON::Any?) : Array(Float64)
      floats = [] of Float64
      return floats unless value
      return floats unless array = value.as_a?

      array.each do |item|
        if float = item.as_f?
          floats << float
        elsif int = item.as_i?
          floats << int.to_f
        end
      end
      floats
    end

    private def bool_text(value : JSON::Any?) : String
      value.try(&.as_bool?) ? "yes" : "-"
    end

    private def compact_json(value : JSON::Any?) : String
      return "-" unless value
      value.to_json
    end

    private def parameter_summary(value : JSON::Any?) : String
      return "-" unless value

      if src_queue = child(value, "src-queue")
        "src=#{json_text(src_queue)} dest=#{json_text(child(value, "dest-queue"))}"
      elsif uri = child(value, "uri")
        "uri=#{json_text(uri)}"
      else
        compact_json(value)
      end
    end

    private def human_bytes(bytes : Int64) : String
      units = {"B", "KiB", "MiB", "GiB", "TiB"}
      value = bytes.to_f
      unit = 0
      while value >= 1024.0 && unit < units.size - 1
        value /= 1024.0
        unit += 1
      end
      unit.zero? ? "#{bytes}B" : "%.1f%s" % {value, units[unit]}
    end

    private def human_duration(ms : Int64) : String
      seconds = ms // 1000
      days = seconds // 86_400
      hours = (seconds % 86_400) // 3600
      minutes = (seconds % 3600) // 60
      return "#{days}d#{hours}h" if days > 0
      return "#{hours}h#{minutes}m" if hours > 0
      "#{minutes}m"
    end
  end
end

LavinMQCtl.tui_launcher = ->(client : HTTP::Client, interval : Float64) {
  LavinMQCtl::TUI.new(client, interval).start
}
