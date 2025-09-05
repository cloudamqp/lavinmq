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

    # See `man 2 flock`
    def acquire
      begin
        @lock.flock_exclusive(blocking: false)
      rescue
        L.info "Data directory locked by '#{@lock.gets_to_end}'"
        L.info "Waiting for file lock to be released"
        @lock.flock_exclusive(blocking: true)
        L.info "Lock acquired"
      end
      L.debug "Data directory lock aquired"
      @lock.truncate
      @lock.print "PID #{Process.pid} @ #{System.hostname}"
    end

    def release
      @lock.truncate
      @lock.flock_unlock
    end

    # Read from the lock file to detect lost lock on networked filesystems (NFS/SMB)
    # See "Lost locks" in `man 2 fcntl`
    def poll
      @lock.read_at(0, 1, &.read_byte) || raise IO::EOFError.new
    rescue ex : IO::Error | ArgumentError
      L.fatal "Lost data dir lock", exception: ex
      exit 4 # 4 for D(dataDir)
    end
  end
end
