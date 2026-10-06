module LavinMQ
  # Make sure that only one instance is using the data directory
  class DataDirLock
    Log = LavinMQ::Log.for "data_dir_lock"

    class Error < Exception; end

    def initialize(data_dir)
      @lock = File.open(File.join(data_dir, ".lock"), "a+")
      @lock.sync = true
      @lock.read_buffering = false
    end

    # Raises `Error` if another process holds the lock. See `man 2 flock`
    def acquire
      begin
        @lock.flock_exclusive(blocking: false)
      rescue ex : IO::Error
        holder = @lock.gets_to_end
        @lock.close
        raise Error.new("Data directory locked by '#{holder}'") if ex.os_error == Errno::EWOULDBLOCK
        raise Error.new("Could not lock data directory: #{ex.message}")
      end
      Log.debug { "Data directory lock acquired" }
      @lock.truncate
      @lock.print "PID #{Process.pid} @ #{System.hostname}"
    end

    def release
      @lock.truncate
      @lock.flock_unlock
    end
  end
end
