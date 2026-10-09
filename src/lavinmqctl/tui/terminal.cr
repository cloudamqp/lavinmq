require "./screen"

lib LibTerminal
  struct Winsize
    ws_row : LibC::UShort
    ws_col : LibC::UShort
    ws_xpixel : LibC::UShort
    ws_ypixel : LibC::UShort
  end

  {% if flag?(:linux) %}
    TIOCGWINSZ = 0x5413_u64
  {% else %}
    TIOCGWINSZ = 0x40087468_u64
  {% end %}

  fun ioctl(fd : LibC::Int, request : LibC::ULong, ...) : LibC::Int
end

class LavinMQCtl
  class TUI
    # Turns bytes read from the terminal into key events
    class KeyParser
      # Longest escape sequence kept while waiting for the rest of it
      MAX_SEQUENCE = 32

      @pending = [] of UInt8
      # In the rest of a sequence that was too long
      @discarding = false

      # An incomplete sequence at the end is kept for the next call, unless
      # *flush*, which a lone ESC needs to become the Escape key
      def parse(bytes : Bytes, flush = false) : Array(KeyEvent)
        @pending.concat(bytes)
        events = [] of KeyEvent
        i = 0
        while i < @pending.size
          if @discarding
            @discarding = !(0x40..0x7e).includes?(@pending[i])
            i += 1
            next
          end
          size, event = next_key(i)
          if size.zero? # incomplete
            break unless flush
            size, event = 1, (@pending[i] == 0x1b ? KeyEvent.new(Key::Escape) : nil)
          end
          events << event if event
          i += size
        end
        @pending = @pending[i..]
        events
      end

      def pending? : Bool
        !@pending.empty?
      end

      # Size of the key at *i* and its event, size 0 if more bytes are needed
      private def next_key(i : Int32) : {Int32, KeyEvent?}
        byte = @pending[i]
        if byte == 0x1b
          escape_sequence(i)
        elsif byte < 0x80
          {1, control_or_char(byte)}
        else
          utf8_char(i)
        end
      end

      private def control_or_char(byte : UInt8) : KeyEvent?
        case byte
        when 0x03       then KeyEvent.new(Key::CtrlC)
        when 0x09       then KeyEvent.new(Key::Tab)
        when 0x0a, 0x0d then KeyEvent.new(Key::Enter)
        when 0x08, 0x7f then KeyEvent.new(Key::Backspace)
        when 0x20..0x7e then KeyEvent.char(byte.unsafe_chr)
        end
      end

      private def utf8_char(i : Int32) : {Int32, KeyEvent?}
        lead = @pending[i]
        size = if lead & 0xe0 == 0xc0
                 2
               elsif lead & 0xf0 == 0xe0
                 3
               elsif lead & 0xf8 == 0xf0
                 4
               else
                 return {1, nil}
               end
        return {0, nil} if i + size > @pending.size
        char = String.new(Slice.new(size) { |j| @pending[i + j] }).char_at(0)
        {size, char == Char::REPLACEMENT ? nil : KeyEvent.char(char)}
      end

      private def escape_sequence(i : Int32) : {Int32, KeyEvent?}
        return {0, nil} if i + 1 >= @pending.size
        case @pending[i + 1]
        when '['.ord then csi(i)
        when 'O'.ord
          return {0, nil} if i + 2 >= @pending.size
          {3, ss3_key(@pending[i + 2].unsafe_chr)}
        else
          # Alt and a key, taken as Escape and the key
          {1, KeyEvent.new(Key::Escape)}
        end
      end

      # ESC [ parameters final, e.g. ESC [ A or ESC [ 5 ~ or ESC [ 1 ; 5 A
      private def csi(i : Int32) : {Int32, KeyEvent?}
        j = i + 2
        while j < @pending.size
          byte = @pending[j]
          if (0x40..0x7e).includes?(byte)
            params = String.new(Slice.new(j - i - 2) { |k| @pending[i + 2 + k] })
            return {j - i + 1, csi_key(params, byte.unsafe_chr)}
          end
          j += 1
          if j - i >= MAX_SEQUENCE
            # Unknown and too long, drop it up to its final byte
            @discarding = true
            return {j - i, nil}
          end
        end
        {0, nil}
      end

      FINAL_KEYS = {
        'A' => Key::Up, 'B' => Key::Down, 'C' => Key::Right, 'D' => Key::Left,
        'H' => Key::Home, 'F' => Key::End, 'Z' => Key::BackTab,
      }
      TILDE_KEYS = {
        "1" => Key::Home, "7" => Key::Home, "4" => Key::End, "8" => Key::End,
        "5" => Key::PageUp, "6" => Key::PageDown,
      }

      private def csi_key(params : String, final : Char) : KeyEvent?
        key = final == '~' ? TILDE_KEYS[params.split(';').first]? : FINAL_KEYS[final]?
        key.try { |k| KeyEvent.new(k) }
      end

      private def ss3_key(final : Char) : KeyEvent?
        FINAL_KEYS[final]?.try { |k| KeyEvent.new(k) }
      end
    end

    # A grid of cells written to the terminal as the escape sequences for
    # what changed since the last render
    class Renderer
      record Cell, char : Char, fg : Color, bg : Color, bold : Bool

      # The right half of a wide character
      WIDE_RIGHT = '\0'
      BLANK      = Cell.new(' ', Color.rgb(255, 255, 255), Color.rgb(0, 0, 0), false)

      getter width : Int32
      getter height : Int32

      def initialize(@io : IO, @width : Int32, @height : Int32, @truecolor = true)
        @back = Array(Cell).new(@width * @height, BLANK)
        @front = Array(Cell?).new(@width * @height, nil)
        @buffer = IO::Memory.new
        @cursor = -1
        @pen = nil.as(Cell?)
      end

      def resize(@width : Int32, @height : Int32) : Nil
        @back = Array(Cell).new(@width * @height, BLANK)
        sync
      end

      # Draws every cell on the next render
      def sync : Nil
        @front = Array(Cell?).new(@width * @height, nil)
      end

      def clear : Nil
        @back.fill(BLANK)
      end

      def set_cell(x : Int32, y : Int32, char : Char, fg : Color, bg : Color, bold : Bool) : Nil
        return unless (0...@width).includes?(x) && (0...@height).includes?(y)
        char = Text.sanitize(char)
        width = Text.width(char)
        char = ' ' if width == 0
        i = y * @width + x
        # Overwriting half of a wide character erases the other half
        @back[i - 1] = @back[i - 1].copy_with(char: ' ') if @back[i].char == WIDE_RIGHT
        @back[i + 1] = @back[i + 1].copy_with(char: ' ') if x + 1 < @width && @back[i + 1].char == WIDE_RIGHT
        if width == 2 && x + 1 >= @width
          char = ' '
          width = 1
        end
        @back[i] = Cell.new(char, fg, bg, bold)
        @back[i + 1] = Cell.new(WIDE_RIGHT, fg, bg, bold) if width == 2
      end

      def render : Nil
        @buffer.clear
        @buffer << "\e[?2026h" # synchronized update, if the terminal supports it
        @cursor = -1
        @pen = nil
        force = false
        @back.each_with_index do |cell, i|
          next if cell.char == WIDE_RIGHT
          next unless force || changed?(i)
          draw(i, cell)
          # Terminals may disagree on the width of anything but ASCII, so
          # redraw the cell after it too
          force = !cell.char.ascii?
        end
        @buffer << "\e[?2026l"
        @io.write(@buffer.to_slice)
        @io.flush
      end

      private def changed?(i : Int32) : Bool
        return true unless @front[i] == @back[i]
        wide?(i) && @front[i + 1] != @back[i + 1]
      end

      private def wide?(i : Int32) : Bool
        i + 1 < @back.size && @back[i + 1].char == WIDE_RIGHT
      end

      private def draw(i : Int32, cell : Cell)
        @buffer << "\e[" << i // @width + 1 << ';' << i % @width + 1 << 'H' unless @cursor == i
        pen = @pen
        unless pen && pen.fg == cell.fg && pen.bg == cell.bg && pen.bold == cell.bold
          write_style(cell)
          @pen = cell
        end
        @buffer << cell.char
        @front[i] = cell
        @front[i + 1] = @back[i + 1] if wide?(i)
        # Position the cursor explicitly after anything but ASCII. With line
        # wrap off the cursor stays in the last column.
        @cursor = cell.char.ascii? && (i + 1) % @width != 0 ? i + 1 : -1
      end

      private def write_style(cell : Cell)
        @buffer << "\e[0"
        @buffer << ";1" if cell.bold
        write_color(38, cell.fg)
        write_color(48, cell.bg)
        @buffer << 'm'
      end

      private def write_color(base : Int32, color : Color)
        if @truecolor
          @buffer << ';' << base << ";2;" << color.r << ';' << color.g << ';' << color.b
        else
          @buffer << ';' << base << ";5;" << Renderer.xterm256(color)
        end
      end

      # The closest color in the xterm 256 color palette's color cube or grays
      def self.xterm256(color : Color) : Int32
        levels = {0, 95, 135, 175, 215, 255}
        cube = {color.r, color.g, color.b}.map { |v| (0..5).min_by { |k| (levels[k] - v.to_i).abs } }
        cube_color = cube.map { |c| levels[c] }
        gray = ((color.r.to_i + color.g + color.b) // 3 - 3).clamp(0, 230) // 10
        gray_value = 8 + gray * 10
        cube_distance = distance(color, cube_color)
        gray_distance = distance(color, {gray_value, gray_value, gray_value})
        if gray_distance < cube_distance
          232 + gray
        else
          16 + 36 * cube[0] + 6 * cube[1] + cube[2]
        end
      end

      private def self.distance(color : Color, other : Tuple(Int32, Int32, Int32)) : Int32
        (color.r.to_i - other[0]) ** 2 + (color.g.to_i - other[1]) ** 2 + (color.b.to_i - other[2]) ** 2
      end
    end

    # The terminal on stdin and stdout, in raw mode on the alternate screen
    # while the TUI runs. It's restored on close, at exit and on SIGTERM,
    # SIGHUP and SIGINT (Ctrl-C is a key in raw mode).
    class TerminalScreen < Screen
      # How long to wait for the rest of an escape sequence before ESC is the Escape key
      ESCAPE_TIMEOUT = 25.milliseconds

      def initialize(@input : IO::FileDescriptor = STDIN, @output : IO::FileDescriptor = STDOUT)
        raise IO::Error.new("lavinmqctl tui needs a terminal") unless @input.tty? && @output.tty?
        @termios = uninitialized LibC::Termios
        @events = Channel(Event).new(64)
        @parser = KeyParser.new
        @closed = false
        width, height = TerminalScreen.size_of(@output)
        colorterm = ENV["COLORTERM"]?.try(&.downcase)
        @renderer = Renderer.new(@output, width, height, truecolor: colorterm.in?("truecolor", "24bit"))

        if LibC.tcgetattr(@input.fd, pointerof(@termios)) != 0
          raise IO::Error.from_errno("tcgetattr")
        end
        at_exit { close }
        raw = @termios
        LibC.cfmakeraw(pointerof(raw))
        LibC.tcsetattr(@input.fd, LibC::TCSANOW, pointerof(raw))
        # Alternate screen, hidden cursor, no line wrap at the last column
        @output.print "\e[?1049h\e[?25l\e[?7l\e[2J"
        @output.flush

        {Signal::TERM, Signal::HUP, Signal::INT}.each do |signal|
          signal.trap do
            close
            exit 128 + signal.value
          end
        end
        Signal::WINCH.trap do
          w, h = TerminalScreen.size_of(@output)
          @events.send ResizeEvent.new(w, h) unless @closed
        end
        spawn(name: "tui input") { read_input }
      end

      def size : {Int32, Int32}
        {@renderer.width, @renderer.height}
      end

      def poll_event(timeout : Time::Span) : Event?
        event = select
        when e = @events.receive?
          e
        when timeout(timeout)
          nil
        end
        @renderer.resize(event.width, event.height) if event.is_a?(ResizeEvent)
        event
      end

      def clear : Nil
        @renderer.clear
      end

      def set_cell(x : Int32, y : Int32, char : Char, fg : Color, bg : Color, bold : Bool) : Nil
        @renderer.set_cell(x, y, char, fg, bg, bold)
      end

      def render : Nil
        @renderer.render
      end

      def sync : Nil
        @output.print "\e[2J"
        @renderer.sync
      end

      def close : Nil
        return if @closed
        @closed = true
        {Signal::TERM, Signal::HUP, Signal::INT, Signal::WINCH}.each(&.reset)
        @output.print "\e[0m\e[?7h\e[?25h\e[?1049l"
        @output.flush
        LibC.tcsetattr(@input.fd, LibC::TCSANOW, pointerof(@termios))
        @events.close
      rescue IO::Error
        # The terminal is gone
      end

      def self.size_of(output : IO::FileDescriptor) : {Int32, Int32}
        size = LibTerminal::Winsize.new
        if LibTerminal.ioctl(output.fd, LibTerminal::TIOCGWINSZ, pointerof(size)) == 0 && size.ws_col > 0 && size.ws_row > 0
          {size.ws_col.to_i32, size.ws_row.to_i32}
        else
          {80, 24}
        end
      end

      private def read_input
        buffer = Bytes.new(1024)
        until @closed
          # Wait briefly for the rest of an escape sequence
          @input.read_timeout = @parser.pending? ? ESCAPE_TIMEOUT : nil
          events = begin
            count = @input.read(buffer)
            break if count.zero?
            @parser.parse(buffer[0, count])
          rescue IO::TimeoutError
            @parser.parse(Bytes.empty, flush: true)
          end
          events.each { |event| @events.send(event) }
        end
      rescue IO::Error | Channel::ClosedError
        # Closed
      ensure
        # The terminal is gone, quit
        unless @closed
          @events.send(KeyEvent.new(Key::CtrlC)) rescue nil
        end
      end
    end
  end
end
