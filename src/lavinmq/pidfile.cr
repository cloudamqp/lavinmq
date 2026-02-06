require "log"

module LavinMQ
  class Pidfile
    Log = LavinMQ::Log.for("launcher")

    private enum State
      Empty
      Stale
      Running
      Invalid
    end

    def initialize(@path : String)
    end

    def acquire : Bool
      return false if @path.empty?
      file = File.open(@path, "a+")
      file.flock_exclusive(blocking: false)
      case check_state(file)
      in State::Empty, State::Stale
        file.truncate
        file.print(Process.pid)
        file.flush
        file.close_on_finalize = false
        path = @path # capture for at_exit closure
        at_exit { File.delete?(path) }
        Log.info { "PID file: #{@path}" }
        true
      in State::Running, State::Invalid
        file.close
        Log.error { "Failed to write PID file #{@path}: file already exists" }
        false
      end
    rescue ex : IO::Error
      file.try &.close
      Log.error { "Failed to write PID file #{@path}: #{ex.message}" }
      false
    rescue ex
      file.try &.close
      Log.error { "Failed to write PID file #{@path}: #{ex.message}" }
      false
    end

    private def check_state(file : File) : State
      file.rewind
      content = file.gets_to_end.strip
      return State::Empty if content.empty?
      pid = content.to_i64?
      return State::Invalid unless pid
      return State::Running if Process.exists?(pid)
      State::Stale
    end
  end
end
