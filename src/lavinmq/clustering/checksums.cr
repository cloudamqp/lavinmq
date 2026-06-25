module LavinMQ
  module Clustering
    class Checksums
      Log = LavinMQ::Log.for "clustering.checksums"
      @checksums = Hash(String, Bytes).new
      # Always-open handle to checksums.sha1, kept open across rewrites so
      # #append never has to check/reopen it: #append writes one line at a time
      # and #store adopts the freshly-renamed file's handle here.
      @checksum_file : File

      def initialize(@data_dir : String)
        Dir.mkdir_p(@data_dir)
        @checksum_file = File.new(checksums_path, "a")
      end

      def store : Nil
        # Write to a temp file and rename, so a crash mid-write can never leave
        # a torn checksums.sha1; restore would then read garbage. The handle
        # follows the inode through the rename (positioned at EOF), so #append
        # can keep using it afterwards.
        tmp = "#{checksums_path}.tmp"
        f = File.new(tmp, "w")
        @checksums.each do |path, hash|
          f.puts "#{hash.hexstring} *#{path}"
        end
        f.flush
        File.rename(tmp, checksums_path)
        @checksum_file.close
        @checksum_file = f
        Log.info { "Wrote #{self.size} checksums to disk" }
      end

      # Set in memory AND persist immediately by appending one line, so hashing
      # progress survives a crash mid-sync (see Client#sync_files). No fsync:
      # the page cache survives a process crash and the cache is only an
      # optimization (a stale entry just triggers a re-fetch, never data loss).
      def append(path : String, hash : Bytes) : Nil
        @checksums[path] = hash
        @checksum_file.puts "#{hash.hexstring} *#{path}"
        @checksum_file.flush
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
        end
        # Discard the on-disk copy now that it's in memory: a crash before the
        # next clean store must not reload these (possibly stale) hashes.
        # Truncate rather than delete so @checksum_file stays valid for #append.
        @checksum_file.truncate(0)
        Log.info { "Restored #{self.size} checksums from disk" }
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
