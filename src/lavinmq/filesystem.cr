require "./mfile"

module LavinMQ
  module FileSystem
    # Rewrite the file at `path` atomically. The block writes the new content
    # to a temporary file next to it, which is fsynced and durably renamed
    # over `path` once the block returns, then closed.
    def self.replace(path : String, mode = "w", & : File ->) : Nil
      replace_keep_open(path, mode) { |file| yield file }.close
    end

    # Like `replace` but returns the installed file still open, for callers
    # that keep appending to it under its final name. Cleans up the temporary
    # file if the block raises.
    def self.replace_keep_open(path : String, mode = "w", & : File ->) : File
      tmp_path = "#{path}.tmp"
      file = File.new(tmp_path, mode)
      begin
        yield file
        file.fsync
        durable_rename(file, path)
      rescue ex
        file.close
        File.delete?(tmp_path)
        raise ex
      end
      file
    end

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

    def self.durable_rename(source : MFile, destination : String) : Nil
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
