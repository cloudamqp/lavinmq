module LavinMQ
  module Clustering
    class Checksums
      Log = LavinMQ::Log.for "clustering.checksums"
      @checksums = Hash(String, Bytes).new
      # Lazily opened append handle used by #append to persist each hash as it
      # is computed; closed/reset by #store before it rewrites the file.
      @append_file : File?

      def initialize(@data_dir : String)
      end

      def store : Nil
        Dir.mkdir_p(@data_dir)
        @append_file.try &.close
        @append_file = nil
        File.open(checksums_path, "w") do |f|
          @checksums.each do |path, hash|
            f.puts "#{hash.hexstring} *#{path}"
          end
        end
        Log.info { "Wrote #{self.size} checksums to disk" }
      end

      # Set in memory AND persist immediately by appending one line, so hashing
      # progress survives a crash mid-sync (see Client#sync_files). No fsync:
      # the page cache survives a process crash and the cache is only an
      # optimization (a stale entry just triggers a re-fetch, never data loss).
      def append(path : String, hash : Bytes) : Nil
        @checksums[path] = hash
        f = (@append_file ||= File.new(checksums_path, "a"))
        f.puts "#{hash.hexstring} *#{path}"
        f.flush
      end

      def restore : Nil
        File.open(checksums_path) do |f|
          loop do
            hash = f.read_string(40).hexbytes
            f.skip(2) # " *"
            path = f.read_line
            @checksums[path] = hash
          rescue IO::EOFError
            break
          end
          f.delete # prevent out-of-date hashes to be restored in the event of a crash
          Log.info { "Restored #{self.size} checksums from disk" }
        end
      rescue File::NotFoundError
        Log.info { "Checksums not found" }
      end

      def []?(path)
        @checksums[path]?
      end

      def []=(path, value)
        @checksums[path] = value
      end

      def delete(path)
        @checksums.delete(path)
      end

      def clear
        @checksums.clear
      end

      def size
        @checksums.size
      end

      private def checksums_path : String
        File.join(@data_dir, "checksums.sha1")
      end
    end
  end
end
