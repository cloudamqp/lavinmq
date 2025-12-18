require "log"

module LavinMQ
  class Pidfile
    Log = LavinMQ::Log.for("launcher")

    enum State
      Empty
      Stale
      Running
      Invalid
    end

    getter path : String
    @lock : File?

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
        @lock = file
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

    def release
      return unless file = @lock
      file.close
      @lock = nil
      File.delete?(@path)
    end

    def check_state(file : File) : State
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
