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
    def self.durable_rename(source : File | MFile, destination : String) : Nil
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
          file.flush
          file.fsync if Config.instance.sync?
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
