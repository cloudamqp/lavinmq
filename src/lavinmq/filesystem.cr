require "./config"
require "./mfile"
require "../stdlib/libc"

module LavinMQ
  # Durable variants of directory entry changes. Data written through a file
  # isn't reachable after a crash until its directory entry is on disk too, so
  # renames and newly created directories fsync the directory they changed.
  # The fsyncs are skipped when `sync` is disabled.
  module FileSystem
    # Syncing more files than this one by one is slower than a single syncfs
    SYNCFS_THRESHOLD = 16

    def self.syncfs(fd : Int32) : Nil
      {% if flag?(:linux) %}
        ret, errno = Fiber.syscall do
          {LibC.syncfs(fd), Errno.value}
        end
        raise IO::Error.from_os_error("syncfs", errno) if ret != 0
      {% else %}
        LibC.sync
      {% end %}
    end

    def self.fsync_dir(path : String) : Nil
      return unless Config.instance.sync?
      File.open(path, &.fsync)
    end

    def self.durable_rename(source : String, destination : String) : Nil
      File.open(source, &.fsync) if Config.instance.sync?
      File.rename(source, destination)
      fsync_rename_dirs(source, destination)
    end

    # Renames the open file in place, so the caller can keep using it under
    # its new path
    def self.durable_rename(source : File | MFile, destination : String) : Nil
      source.flush if source.is_a?(File)
      source.fsync if Config.instance.sync?
      source_path = source.path
      source.rename(destination)
      fsync_rename_dirs(source_path, destination)
    end

    # Atomically replaces `path` with what the block writes, via a tmp file
    def self.replace(path : String, & : File ->) : Nil
      tmp_path = "#{path}.tmp"
      begin
        File.open(tmp_path, "w") do |file|
          yield file
          durable_rename(file, path)
        end
      ensure
        File.delete?(tmp_path)
      end
    end

    # Like `Dir.mkdir_p`, but also fsyncs the parent of each created directory
    def self.mkdir_p(path : String) : Nil
      return if Dir.exists?(path)
      mkdir_p(File.dirname(path))
      begin
        Dir.mkdir(path)
      rescue File::AlreadyExistsError
        return
      end
      fsync_dir(File.dirname(path))
    end

    private def self.fsync_rename_dirs(source : String, destination : String) : Nil
      return unless Config.instance.sync?
      source_dir = File.dirname(source)
      destination_dir = File.dirname(destination)
      fsync_dir(destination_dir)
      fsync_dir(source_dir) unless source_dir == destination_dir
    end
  end
end
