module LavinMQ
  module FileSystem
    # Shared by the segments in a message store. The descriptor lives as long
    # as the store; syncing entries never opens or walks ancestor directories.
    class Directory
      @lock = Mutex.new

      def initialize(path : String)
        @file = File.open(path)
      end

      def fsync : Nil
        @lock.synchronize { @file.fsync }
      end

      def close : Nil
        @lock.synchronize { @file.close }
      end

      # Queue deletion may follow close. Reopen once for that teardown, then
      # reuse the descriptor for every removed segment before closing it again.
      def reopen : Nil
        @lock.synchronize do
          @file = File.open(@file.path) if @file.closed?
        end
      end
    end

    # Sync and atomically install a file and make the changed directory
    # entry durable before returning. Most callers rename within one directory;
    # syncing both parents also makes cross-directory renames safe.
    def self.durable_rename(source : String, destination : String) : Nil
      File.open(source, &.fsync) if Config.instance.sync?
      File.rename(source, destination)
      fsync_rename_dirs(source, destination)
    end

    # Preserve File#rename's path bookkeeping for callers that keep using the
    # open handle after installing it under its final name.
    def self.durable_rename(source : File | MFile, destination : String) : Nil
      source.flush if source.is_a?(File)
      source.fsync if Config.instance.sync?
      source_path = source.path
      source.rename(destination)
      fsync_rename_dirs(source_path, destination)
    end

    # Write a complete replacement without exposing partial contents at the
    # destination. Callers serialize writes to the same destination.
    def self.replace(path : String, & : File ->) : Nil
      temporary = "#{path}.tmp"
      begin
        File.open(temporary, "w") do |file|
          yield file
          durable_rename(file, path)
        end
      ensure
        File.delete?(temporary)
      end
    end

    private def self.fsync_rename_dirs(source : String, destination : String) : Nil
      return unless Config.instance.sync?
      source_dir = File.dirname(source)
      destination_dir = File.dirname(destination)
      File.open(destination_dir, &.fsync)
      File.open(source_dir, &.fsync) unless source_dir == destination_dir
    end
  end
end
