class LavinMQCtl
  class TUI
    # The broker's log from /api/logs, the entries it keeps in memory,
    # oldest first
    @log_lines = [] of String
    # Rows above the newest one at the bottom, 0 follows new entries
    @log_scroll = 0
    @log_filter = ""

    # Like "2026-10-10 23:43:08 UTC [WARN] lmq.launcher - message"
    LOG_LINE = /\A\d{4}-(\d\d-\d\d \d\d:\d\d:\d\d)\S* \S+ \[([A-Z]+)\] (\S*) - (.*)\z/

    private def fetch_logs
      if body = fetch_body("/api/logs", "logs")
        @log_lines = body.lines
      end
    end

    # Up and Down scroll a row, Home goes to the oldest entry and End to
    # the newest, which follows new entries
    private def log_key(key : Key)
      rows = table_rows
      case key
      when .up?        then scroll_logs(1)
      when .down?      then scroll_logs(-1)
      when .page_up?   then scroll_logs(rows)
      when .page_down? then scroll_logs(-rows)
      when .home?      then @log_scroll = Int32::MAX
      when .end?       then @log_scroll = 0
      end
    end

    # Limited when drawn, as it depends on the terminal size
    private def scroll_logs(rows : Int32)
      @log_scroll = (@log_scroll.to_i64 + rows).clamp(0, Int32::MAX).to_i32
    end

    private def draw_logs
      rect = Rect.new(1, 2, @width - 2, @height - 4)
      width = rect.inner_width - 4
      entries = @log_filter.empty? ? @log_lines : @log_lines.select(&.downcase.includes?(@log_filter.downcase))
      rows = entries.flat_map { |line| log_rows(line, width) }
      visible = {rect.inner_height - 2, 0}.max
      @log_scroll = @log_scroll.clamp(0, {rows.size - visible, 0}.max)
      note = String.build do |s|
        s << "newest " << @log_scroll << " rows down, End follows" if @log_scroll > 0
        s << "  filter \"" << @log_filter << '"' unless @log_filter.empty?
      end
      draw_panel(rect, "Logs", note.lstrip, entries.size.format)
      if rows.empty?
        message = if @log_lines.empty?
                    "No log entries. The log is only shown to users with the administrator tag."
                  else
                    "Nothing matches the filter"
                  end
        print_at(rect.inner_x + 2, rect.inner_y + 1, message, MUTED_FG, PANEL_BG, max_width: width)
        return
      end
      first = {rows.size - visible - @log_scroll, 0}.max
      rows[first, visible].each_with_index do |segments, i|
        x = rect.inner_x + 2
        segments.each do |(text, color)|
          x += print_at(x, rect.inner_y + 1 + i, text, color, PANEL_BG, color == RED, rect.inner_x + 2 + width - x)
        end
      end
    end

    # A log entry as screen rows of colored text, wrapped at *width*
    private def log_rows(line : String, width : Int32) : Array(Array({String, Color}))
      return [[{line, TEXT_FG}]] if width < 20
      if match = LOG_LINE.match(line)
        time, severity, source, message = match[1], match[2], match[3], match[4]
        prefix = [{time + " ", MUTED_FG}, {severity.ljust(5) + " ", severity_color(severity)}, {source + " ", MUTED_FG}]
        indent = time.size + 7
      else
        # A message's following lines
        prefix = [] of {String, Color}
        message = line
        indent = 2
      end
      used = prefix.sum { |(text, _)| Text.width(text) }
      rows = [] of Array({String, Color})
      row = prefix
      rest = message
      loop do
        room = {width - used, 1}.max
        part, rest = split_at_width(rest, room)
        row << {part, TEXT_FG}
        rows << row
        break if rest.empty?
        row = [{" " * indent, TEXT_FG}]
        used = indent
      end
      rows
    end

    # The first *width* cells of *text*, at least a character, and the rest
    private def split_at_width(text : String, width : Int32) : {String, String}
      used = 0
      text.each_char_with_index do |char, i|
        used += Text.width(Text.sanitize(char))
        return {text[0, {i, 1}.max], text[{i, 1}.max..]} if used > width
      end
      {text, ""}
    end

    private def severity_color(severity : String) : Color
      case severity
      when "ERROR", "FATAL" then RED
      when "WARN"           then YELLOW
      when "INFO", "NOTICE" then GREEN
      else                       MUTED_FG
      end
    end
  end
end
