module LavinMQ
  # Makes sure that only one instance is using the data directory
  class DataDirLock
    Log = LavinMQ::Log.for "data_dir_lock"

    # Another process holds the lock
    class Locked < Exception; end

    def initialize(@data_dir : String)
      @lock = File.open(File.join(data_dir, ".lock"), "a+")
      @lock.sync = true
      @lock.read_buffering = false
    end

    # Raises Locked if another process holds the lock, instead of waiting for
    # it: two instances on one data dir is a mistake, not a standby. See `man 2 flock`
    def acquire
      begin
        @lock.flock_exclusive(blocking: false)
      rescue ex : IO::Error
        raise ex unless ex.os_error.in?(Errno::EAGAIN, Errno::EWOULDBLOCK)
        holder = @lock.gets_to_end
        holder = "another process" if holder.empty?
        raise Locked.new("Data directory #{@data_dir} is locked by #{holder}")
      end
      Log.debug { "Data directory lock aquired" }
      @lock.truncate
      @lock.print "PID #{Process.pid} @ #{System.hostname}"
    end

    def release
      @lock.truncate
      @lock.flock_unlock
    end
  end
end
