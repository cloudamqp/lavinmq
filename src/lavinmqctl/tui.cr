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

    def initialize(@client : HTTP::Client, @interval : Float64 = 1.0, @screen : Screen = TermisuScreen.new)
      @running = true
      @width = 0
      @height = 0
      @page = :overview
      @last_error = nil.as(String?)
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

      header_bg = Termisu::Color.blue
      header_fg = Termisu::Color.white
      header_text = " LavinMQ TUI | #{page} | Node: #{node} | v#{version} "

      @width.times do |i|
        @screen.set_cell(i, 0, ' ', header_fg, header_bg, Termisu::Attribute::None)
      end
      print_at(0, 0, header_text, header_fg, header_bg, Termisu::Attribute::Bold)
    end

    private def render_footer
      y = @height - 1
      return if y < 0

      footer_bg = Termisu::Color.white
      footer_fg = Termisu::Color.black
      nav = PAGES.map { |p| "[#{p[:key]}]#{p[:nav]}" }.join(" ")
      footer_text = " #{nav} [q] Quit "

      @width.times do |i|
        @screen.set_cell(i, y, ' ', footer_fg, footer_bg, Termisu::Attribute::None)
      end
      if error = @last_error
        error_text = "| #{error} "
        nav_width = {@width - error_text.size, 0}.max
        print_at(0, y, fit(footer_text, nav_width), footer_fg, footer_bg)
        print_at(nav_width, y, error_text, Termisu::Color.red, footer_bg, Termisu::Attribute::Bold)
      else
        print_at(0, y, footer_text, footer_fg, footer_bg)
      end
    end

    private def render_overview_page(overview : JSON::Any?)
      unless overview
        render_empty("Overview unavailable")
        return
      end

      y = 2
      y = render_section("Object totals", y)
      if totals = child(overview, "object_totals")
        render_kv_rows(y, [
          {"Connections", json_text(child(totals, "connections"))},
          {"Channels", json_text(child(totals, "channels"))},
          {"Queues", json_text(child(totals, "queues"))},
          {"Consumers", json_text(child(totals, "consumers"))},
          {"Exchanges", json_text(child(totals, "exchanges"))},
          {"Bindings", json_text(child(totals, "bindings"))},
        ])
      end

      y += 5
      y = render_section("Messages", y)
      if queue_totals = child(overview, "queue_totals")
        rate = json_float(child(child(child(overview, "message_stats"), "publish_details"), "rate"))
        render_kv_rows(y, [
          {"Total", json_text(child(queue_totals, "messages"))},
          {"Ready", json_text(child(queue_totals, "messages_ready"))},
          {"Unacked", json_text(child(queue_totals, "messages_unacknowledged"))},
          {"Publish rate", "%.1f/s" % rate},
        ])
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
      y = render_section(title, 2)
      render_row(y, headers, widths, Termisu::Color.green, Termisu::Attribute::Underline)
      y += 1

      if rows.empty?
        print_at(2, y + 1, "No data")
        return
      end

      rows.each do |row|
        break if y >= @height - 2
        render_row(y, row, widths)
        y += 1
      end
    end

    private def render_row(y : Int32, values : Array(String), widths : Array(Int32), fg = Termisu::Color.default, attr = Termisu::Attribute::None)
      x = 2
      values.each_with_index do |value, i|
        width = widths[i]? || 10
        print_at(x, y, fit(value, width), fg, Termisu::Color.default, attr)
        x += width + 1
        break if x >= @width
      end
    end

    private def render_section(title : String, y : Int32) : Int32
      print_at(2, y, title, Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
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
      print_at(2, 3, message, Termisu::Color.red, Termisu::Color.default, Termisu::Attribute::Bold)
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
