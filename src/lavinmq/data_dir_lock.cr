module LavinMQ
  # Make sure that only one instance is using the data directory
  # Can work as a poor mans cluster where the master nodes acquires
  # a file lock on a shared file system like NFS
  class DataDirLock
    Log = LavinMQ::Log.for "data_dir_lock"

    def initialize(data_dir)
      @lock = File.open(File.join(data_dir, ".lock"), "a+")
      @lock.sync = true
      @lock.read_buffering = false
    end

    def acquire
      begin
        @lock.flock_exclusive(blocking: false)
      rescue
        Log.info { "Data directory locked by '#{@lock.gets_to_end}'" }
        Log.info { "Waiting for file lock to be released" }
        @lock.flock_exclusive(blocking: true)
        Log.info { "Lock acquired" }
      end
      @lock.truncate
      @lock.print "PID #{Process.pid} @ #{System.hostname}"
      @lock.fsync
    end

    def release
      @lock.truncate
      @lock.fsync
      @lock.flock_unlock
    end

    # Read from the lock file to detect lost lock
    # See "Lost locks" in `man 2 fcntl`
    def poll
      @lock.read_at(0, 1, &.read_byte) || raise IO::EOFError.new
    rescue ex : IO::Error | ArgumentError
      abort "ERROR: Lost data directory lock! #{ex.inspect}"
    end
  end
end
