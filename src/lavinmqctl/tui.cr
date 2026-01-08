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
      response = @client.get("/api/overview")
      if response.status_code == 200
        JSON.parse(response.body)
      else
        nil
      end
    rescue
      nil
    end

    private def fetch_queues
      response = @client.get("/api/queues?page=1&page_size=20&sort=message_stats.publish_details.rate&sort_reverse=true")
      if response.status_code == 200
        JSON.parse(response.body)["items"].as_a
      else
        [] of JSON::Any
      end
    rescue
      [] of JSON::Any
    end

    private def render
      @termisu.clear # Clear buffer
      
      # Ensure size is up to date (though resize event handles it mostly)
      @width, @height = @termisu.size

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

    private def print_at(x, y, text, fg = Termisu::Color.default, bg = Termisu::Color.default, attr = Termisu::Attribute::None)
      text.each_char_with_index do |char, i|
        next if x + i >= @width
        @termisu.set_cell(x + i, y, char, fg, bg, attr)
      end
    end

    private def render_header(overview)
      version = overview.try { |o| o["lavinmq_version"]? } || "?"
      node = overview.try { |o| o["node"]? } || "Unknown"
      
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
    end

    private def render_overview_page(overview)
      return unless overview
      
      y = 2
      if totals = overview["object_totals"]?
        print_at(2, y, "Totals:", Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
        y += 2
        
        conns = totals["connections"]?
        chans = totals["channels"]?
        queues = totals["queues"]?
        consumers = totals["consumers"]?
        
        print_at(4, y, "Connections: #{conns}")
        print_at(30, y, "Channels:    #{chans}")
        y += 1
        print_at(4, y, "Queues:      #{queues}")
        print_at(30, y, "Consumers:   #{consumers}")
      end
      
      y += 3
      if queue_totals = overview["queue_totals"]?
        print_at(2, y, "Messages:", Termisu::Color.yellow, Termisu::Color.default, Termisu::Attribute::Bold)
        y += 2
        
        msgs = queue_totals["messages"]?
        ready = queue_totals["messages_ready"]?
        unacked = queue_totals["messages_unacknowledged"]?
        
        # Rate is global publish rate
        rate = overview.dig?("message_stats", "publish_details", "rate").try(&.as_f?) || 0.0
        
        print_at(4, y, "Total:       #{msgs}")
        print_at(30, y, "Rate:        #{rate}/s")
        y += 1
        print_at(4, y, "Ready:       #{ready}")
        y += 1
        print_at(4, y, "Unacked:     #{unacked}")
      end
    end

    private def render_queues_page(queues)
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
        
        name = q["name"].as_s
        msgs = q["messages"].as_i
        ready = q["messages_ready"].as_i
        rate = q.dig?("message_stats", "publish_details", "rate").try(&.as_f?) || 0.0
        
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
  end
end
