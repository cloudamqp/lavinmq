require "termisu"
require "./screen"

class LavinMQCtl
  class TUI
    class TermisuScreen < Screen
      def initialize
        # Termisu otherwise logs everything in the process at debug level to
        # /tmp/termisu.log, a fixed path that it opens following symlinks
        Termisu::Logging.configured = true
        @termisu = Termisu.new
      end

      def size : {Int32, Int32}
        @termisu.size
      end

      def poll_event(timeout : Time::Span) : Event?
        case event = @termisu.poll_event(timeout.total_milliseconds.to_i)
        when Termisu::Event::Key    then key_event(event)
        when Termisu::Event::Resize then ResizeEvent.new(event.width, event.height)
        end
      end

      KEYS = {
        Termisu::Input::Key::Up        => Key::Up,
        Termisu::Input::Key::Down      => Key::Down,
        Termisu::Input::Key::Left      => Key::Left,
        Termisu::Input::Key::Right     => Key::Right,
        Termisu::Input::Key::PageUp    => Key::PageUp,
        Termisu::Input::Key::PageDown  => Key::PageDown,
        Termisu::Input::Key::Home      => Key::Home,
        Termisu::Input::Key::End       => Key::End,
        Termisu::Input::Key::Enter     => Key::Enter,
        Termisu::Input::Key::Escape    => Key::Escape,
        Termisu::Input::Key::Backspace => Key::Backspace,
        Termisu::Input::Key::Tab       => Key::Tab,
        Termisu::Input::Key::BackTab   => Key::BackTab,
      }

      private def key_event(event : Termisu::Event::Key) : KeyEvent?
        return KeyEvent.new(Key::CtrlC) if event.ctrl_c?
        if key = KEYS[event.key]?
          KeyEvent.new(key)
        else
          event.char.try { |char| KeyEvent.char(char) }
        end
      end

      def clear : Nil
        @termisu.clear
      end

      def set_cell(x : Int32, y : Int32, char : Char, fg : Color, bg : Color, bold : Bool) : Nil
        attr = bold ? Termisu::Attribute::Bold : Termisu::Attribute::None
        @termisu.set_cell(x, y, char, Termisu::Color.rgb(fg.r, fg.g, fg.b), Termisu::Color.rgb(bg.r, bg.g, bg.b), attr)
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
    end
  end
end
