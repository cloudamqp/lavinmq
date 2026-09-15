module LavinMQ
  module FileSystem
    # Atomically install an already-synced file and make the changed directory
    # entry durable before returning. Most callers rename within one directory;
    # syncing both parents also makes cross-directory renames safe.
    def self.durable_rename(source : String, destination : String) : Nil
      File.rename(source, destination)
      fsync_rename_dirs(source, destination)
    end

    # Preserve File#rename's path bookkeeping for callers that keep using the
    # open handle after installing it under its final name.
    def self.durable_rename(source : File, destination : String) : Nil
      source_path = source.path
      source.rename(destination)
      fsync_rename_dirs(source_path, destination)
    end

    private def self.fsync_rename_dirs(source : String, destination : String) : Nil
      source_dir = File.dirname(source)
      destination_dir = File.dirname(destination)
      File.open(destination_dir, &.fsync)
      File.open(source_dir, &.fsync) unless source_dir == destination_dir
    end
  end
end
