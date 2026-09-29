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

    @@mkdir_lock = Mutex.new

    # Persist each newly created directory's entry in its parent, including
    # ancestors created by mkdir_p. Serializing creation prevents another
    # caller using a new directory before its parent barrier completes.
    def self.mkdir_p(path : String) : Nil
      @@mkdir_lock.synchronize { mkdir_p_locked(File.expand_path(path)) }
    end

    private def self.mkdir_p_locked(path : String) : Nil
      return if Dir.exists?(path)
      parent = File.dirname(path)
      mkdir_p_locked(parent)
      begin
        Dir.mkdir(path)
      rescue ex : File::AlreadyExistsError
        raise ex unless Dir.exists?(path)
      end
      sync_created_directory(parent)
    end

    private def self.sync_created_directory(path : String) : Nil
      File.open(path, &.fsync)
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
    def self.durable_rename(source : File | MFile, destination : String, *, directory : Directory? = nil) : Nil
      source.flush if source.is_a?(File)
      source.fsync if Config.instance.sync?
      source_path = source.path
      source.rename(destination)
      if directory && File.dirname(source_path) == File.dirname(destination)
        directory.fsync
      else
        fsync_rename_dirs(source_path, destination)
      end
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
      # Namespace changes remain durable even when data syncing is disabled.
      source_dir = File.dirname(source)
      destination_dir = File.dirname(destination)
      File.open(destination_dir, &.fsync)
      File.open(source_dir, &.fsync) unless source_dir == destination_dir
    end
  end
end
