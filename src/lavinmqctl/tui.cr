require "json"
require "http/client"
require "termisu"

class LavinMQCtl
  class TUI
    def initialize(@client : HTTP::Client, @interval : Float64 = 1.0)
      @running = true
      @termisu = Termisu.new
      @width = 0
      @height = 0
      @page = :overview
      @last_error = nil.as(String?)
    end

    def start
      @width, @height = @termisu.size
      render # Initial render

      poll_ms = (@interval * 1000).to_i

      loop do
        # Poll for event with timeout for data refresh
        if event = @termisu.poll_event(poll_ms)
          case event
          when Termisu::Event::Key
            handle_key(event)
          when Termisu::Event::Resize
            @width = event.width
            @height = event.height
            @termisu.sync # Force full redraw on resize
            render
          end
        else
          # Timeout -> Refresh data
          render
        end

        break unless @running
      end
    ensure
      @termisu.close
    end

    private def handle_key(event : Termisu::Event::Key)
      if event.ctrl_c? || (event.char == 'q')
        @running = false
        return
      end

      case event.char
      when '1'
        @page = :overview
        render
      when '2'
        @page = :queues
        render
      end
    end

    private def fetch_overview
      fetch_json("/api/overview", "overview")
    end

    private def fetch_queues
      data = fetch_json("/api/queues?page=1&page_size=20&sort=message_stats.publish_details.rate&sort_reverse=true", "queues")
      return [] of JSON::Any unless data

      items = child(data, "items")
      unless queues = items.try(&.as_a?)
        record_error("queues: missing items")
        return [] of JSON::Any
      end
      queues
    end

    private def render
      @termisu.clear # Clear buffer

      # Ensure size is up to date (though resize event handles it mostly)
      @width, @height = @termisu.size
      @last_error = nil

      overview = fetch_overview

      render_header(overview)

      case @page
      when :overview
        render_overview_page(overview)
      when :queues
        queues = fetch_queues
        render_queues_page(queues)
      end

      render_footer

      @termisu.render
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

    private def print_at(x, y, text, fg = Termisu::Color.default, bg = Termisu::Color.default, attr = Termisu::Attribute::None)
      text.each_char_with_index do |char, i|
        next if x + i >= @width
        @termisu.set_cell(x + i, y, char, fg, bg, attr)
      end
    end

    private def render_header(overview : JSON::Any?)
      version = json_text(child(overview, "lavinmq_version"), "?")
      node = json_text(child(overview, "node"), "Unknown")

      header_bg = Termisu::Color.blue
      header_fg = Termisu::Color.white
      header_text = " LavinMQ TUI | Node: #{node} | v#{version} "

      (@width).times do |i|
        @termisu.set_cell(i, 0, ' ', header_fg, header_bg)
      end
      print_at(0, 0, header_text, header_fg, header_bg, Termisu::Attribute::Bold)
    end

    private def render_footer
      footer_text = " [q] Quit | [1] Overview | [2] Queues "
      y = @height - 1

      # Safety check for small terminals
      return if y < 0

      (@width).times do |i|
        @termisu.set_cell(i, y, ' ', Termisu::Color.black, Termisu::Color.white)
      end
      print_at(0, y, footer_text, Termisu::Color.black, Termisu::Color.white)
      if error = @last_error
        print_at(footer_text.size, y, "| #{error} ", Termisu::Color.red, Termisu::Color.white)
      end
    end

    private def render_overview_page(overview : JSON::Any?)
      return unless overview

      y = 2
      if totals = child(overview, "object_totals")
        print_at(2, y, "Totals:", Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
        y += 2

        conns = json_text(child(totals, "connections"))
        chans = json_text(child(totals, "channels"))
        queues = json_text(child(totals, "queues"))
        consumers = json_text(child(totals, "consumers"))

        print_at(4, y, "Connections: #{conns}")
        print_at(30, y, "Channels:    #{chans}")
        y += 1
        print_at(4, y, "Queues:      #{queues}")
        print_at(30, y, "Consumers:   #{consumers}")
      end

      y += 3
      if queue_totals = child(overview, "queue_totals")
        print_at(2, y, "Messages:", Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
        y += 2

        msgs = json_text(child(queue_totals, "messages"))
        ready = json_text(child(queue_totals, "messages_ready"))
        unacked = json_text(child(queue_totals, "messages_unacknowledged"))

        # Rate is global publish rate
        rate = json_float(child(child(child(overview, "message_stats"), "publish_details"), "rate"))

        print_at(4, y, "Total:       #{msgs}")
        print_at(30, y, "Rate:        #{rate}/s")
        y += 1
        print_at(4, y, "Ready:       #{ready}")
        y += 1
        print_at(4, y, "Unacked:     #{unacked}")
      end
    end

    private def render_queues_page(queues : Array(JSON::Any))
      y = 2
      print_at(2, y, "Top Queues:", Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
      y += 2

      headers = ["Name", "Messages", "Ready", "Rate/s"]
      col_widths = [30, 10, 10, 10]
      x = 2
      headers.each_with_index do |h, i|
        print_at(x, y, h, Termisu::Color.green, Termisu::Color.default, Termisu::Attribute::Underline)
        x += col_widths[i] + 2
      end
      y += 1

      queues.each do |q|
        break if y >= @height - 2

        name = json_text(child(q, "name"))
        msgs = json_text(child(q, "messages"))
        ready = json_text(child(q, "messages_ready"))
        rate = json_float(child(child(child(q, "message_stats"), "publish_details"), "rate"))

        name = name[0..27] + ".." if name.size > 29

        x = 2
        print_at(x, y, name)
        x += col_widths[0] + 2
        print_at(x, y, msgs.to_s)
        x += col_widths[1] + 2
        print_at(x, y, ready.to_s)
        x += col_widths[2] + 2
        print_at(x, y, "%.1f" % rate)

        y += 1
      end
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
  end
end

LavinMQCtl.tui_launcher = ->(client : HTTP::Client, interval : Float64) {
  LavinMQCtl::TUI.new(client, interval).start
}
