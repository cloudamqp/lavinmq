class LavinMQCtl
  class TUI
    record Color, r : UInt8, g : UInt8, b : UInt8 do
      def self.rgb(r : Int, g : Int, b : Int) : Color
        new(r.to_u8, g.to_u8, b.to_u8)
      end
    end

    enum Key
      Char
      Up
      Down
      Left
      Right
      PageUp
      PageDown
      Home
      End
      Enter
      Escape
      Backspace
      Tab
      BackTab
      CtrlC
    end

    record KeyEvent, key : Key, char : Char = '\0' do
      def self.char(char : Char) : KeyEvent
        new(Key::Char, char)
      end
    end

    record ResizeEvent, width : Int32, height : Int32

    alias Event = KeyEvent | ResizeEvent

    # Where the TUI draws, a terminal or a fake one in specs. A wide character
    # covers the cell to its right too, which the TUI then doesn't write to.
    abstract class Screen
      abstract def size : {Int32, Int32}
      abstract def poll_event(timeout : Time::Span) : Event?
      abstract def clear : Nil
      abstract def set_cell(x : Int32, y : Int32, char : Char, fg : Color, bg : Color, bold : Bool) : Nil
      abstract def render : Nil
      # Redraws everything on the next render, as after a resize
      abstract def sync : Nil
      abstract def close : Nil
    end

    # Terminal cell widths of characters. The TUI shows strings that remote
    # clients control, like consumer tags and MQTT client ids, so control
    # characters are never written to the terminal.
    module Text
      # East Asian Wide and Fullwidth characters, and emoji presented as such
      WIDE = {
        {0x1100, 0x115F}, {0x231A, 0x231B}, {0x2329, 0x232A}, {0x23E9, 0x23EC}, {0x23F0, 0x23F0},
        {0x23F3, 0x23F3}, {0x25FD, 0x25FE}, {0x2614, 0x2615}, {0x2648, 0x2653}, {0x267F, 0x267F},
        {0x2693, 0x2693}, {0x26A1, 0x26A1}, {0x26AA, 0x26AB}, {0x26BD, 0x26BE}, {0x26C4, 0x26C5},
        {0x26CE, 0x26CE}, {0x26D4, 0x26D4}, {0x26EA, 0x26EA}, {0x26F2, 0x26F3}, {0x26F5, 0x26F5},
        {0x26FA, 0x26FA}, {0x26FD, 0x26FD}, {0x2705, 0x2705}, {0x270A, 0x270B}, {0x2728, 0x2728},
        {0x274C, 0x274C}, {0x274E, 0x274E}, {0x2753, 0x2755}, {0x2757, 0x2757}, {0x2795, 0x2797},
        {0x27B0, 0x27B0}, {0x27BF, 0x27BF}, {0x2B1B, 0x2B1C}, {0x2B50, 0x2B50}, {0x2B55, 0x2B55},
        {0x2E80, 0x303E}, {0x3041, 0x33FF}, {0x3400, 0x4DBF}, {0x4E00, 0x9FFF}, {0xA000, 0xA4CF},
        {0xA960, 0xA97F}, {0xAC00, 0xD7A3}, {0xF900, 0xFAFF}, {0xFE10, 0xFE19}, {0xFE30, 0xFE6F},
        {0xFF00, 0xFF60}, {0xFFE0, 0xFFE6}, {0x16FE0, 0x16FE4}, {0x16FF0, 0x16FF1}, {0x17000, 0x18CD5},
        {0x18D00, 0x18D08}, {0x1AFF0, 0x1AFFE}, {0x1B000, 0x1B2FB}, {0x1F004, 0x1F004}, {0x1F0CF, 0x1F0CF},
        {0x1F18E, 0x1F18E}, {0x1F191, 0x1F19A}, {0x1F200, 0x1F202}, {0x1F210, 0x1F23B}, {0x1F240, 0x1F248},
        {0x1F250, 0x1F251}, {0x1F260, 0x1F265}, {0x1F300, 0x1F320}, {0x1F32D, 0x1F335}, {0x1F337, 0x1F37C},
        {0x1F37E, 0x1F393}, {0x1F3A0, 0x1F3CA}, {0x1F3CF, 0x1F3D3}, {0x1F3E0, 0x1F3F0}, {0x1F3F4, 0x1F3F4},
        {0x1F3F8, 0x1F43E}, {0x1F440, 0x1F440}, {0x1F442, 0x1F4FC}, {0x1F4FF, 0x1F53D}, {0x1F54B, 0x1F54E},
        {0x1F550, 0x1F567}, {0x1F57A, 0x1F57A}, {0x1F595, 0x1F596}, {0x1F5A4, 0x1F5A4}, {0x1F5FB, 0x1F64F},
        {0x1F680, 0x1F6C5}, {0x1F6CC, 0x1F6CC}, {0x1F6D0, 0x1F6D2}, {0x1F6D5, 0x1F6D7}, {0x1F6DC, 0x1F6DF},
        {0x1F6EB, 0x1F6EC}, {0x1F6F4, 0x1F6FC}, {0x1F7E0, 0x1F7EB}, {0x1F7F0, 0x1F7F0}, {0x1F90C, 0x1F93A},
        {0x1F93C, 0x1F945}, {0x1F947, 0x1F9FF}, {0x1FA70, 0x1FA7C}, {0x1FA80, 0x1FA89}, {0x1FA8F, 0x1FAC6},
        {0x1FACE, 0x1FADC}, {0x1FADF, 0x1FAE9}, {0x1FAF0, 0x1FAF8}, {0x20000, 0x2FFFD}, {0x30000, 0x3FFFD},
      }

      # Control characters, line separators and invalid UTF-8 (decoded as
      # U+FFFD, whose width terminals disagree on) are shown as '?'
      def self.sanitize(char : Char) : Char
        ord = char.ord
        if ord < 0x20 || (0x7F..0x9F).includes?(ord) || ord == 0x2028 || ord == 0x2029 || char == Char::REPLACEMENT
          '?'
        else
          char
        end
      end

      # 0 for combining marks and format characters (zero width spaces and
      # joiners, bidi controls), which are left out, 2 for wide characters
      def self.width(char : Char) : Int32
        ord = char.ord
        return 1 if ord < 0x300
        return 0 if char.mark? || format?(ord)
        wide?(ord) ? 2 : 1
      end

      def self.width(text : String) : Int32
        text.each_char.sum { |char| width(sanitize(char)) }
      end

      private def self.format?(ord : Int32) : Bool
        ord == 0xAD || ord == 0xFEFF || (0x200B..0x200F).includes?(ord) || (0x202A..0x202E).includes?(ord) ||
          (0x2060..0x206F).includes?(ord) || (0xFFF9..0xFFFB).includes?(ord) || (0xE0000..0xE007F).includes?(ord)
      end

      private def self.wide?(ord : Int32) : Bool
        return false if ord < WIDE[0][0]
        index = WIDE.bsearch_index { |(_, last)| last >= ord }
        !index.nil? && WIDE[index][0] <= ord
      end
    end
  end
end
