module LavinMQ
  module Clustering
    class Checksums
      Log = LavinMQ::Log.for "clustering.checksums"

      # `size` is the number of bytes the hash covers, when known. Entries
      # without a size (older checksums.sha1 files, follower-written entries)
      # can never be served for a sized lookup (#hash_for?).
      record Entry, hash : Bytes, size : Int64? do
        # One line per entry: "<hash> <size> *<path>", or "<hash> *<path>"
        # when the size is unknown. The path is the caller's hash key, so it
        # travels alongside the entry rather than being stored in it.
        def to_io(io : IO, path : String) : Nil
          io << hash.hexstring
          if s = size
            io << ' ' << s
          end
          io << " *" << path << '\n'
        end

        # Returns nil for malformed lines. Raises IO::EOFError at end of file.
        def self.from_io(io : IO) : {String, Entry}?
          hash = io.read_string(40).hexbytes
          rest = io.read_line
          if rest.starts_with?(" *")
            {rest[2..], new(hash, nil)}
          elsif idx = rest.index(" *", 1)
            {rest[idx + 2..], new(hash, rest[1...idx].to_i64?)}
          end
        end
      end

      @checksums = Hash(String, Entry).new
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
        @checksums.each do |path, entry|
          entry.to_io(f, path)
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
      def append(path : String, hash : Bytes, size : Int64? = nil) : Nil
        entry = Entry.new(hash, size)
        @checksums[path] = entry
        entry.to_io(@checksum_file, path)
        @checksum_file.flush
      end

      def restore : Nil
        File.open(checksums_path) do |f|
          loop do
            if parsed = Entry.from_io(f)
              path, entry = parsed
              @checksums[path] = entry
            end
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

      def []?(path) : Bytes?
        @checksums[path]?.try &.hash
      end

      # The hash for `path` only if it covers exactly `size` bytes.
      def hash_for?(path, size : Int64) : Bytes?
        if entry = @checksums[path]?
          entry.hash if entry.size == size
        end
      end

      def []=(path, hash : Bytes)
        @checksums[path] = Entry.new(hash, nil)
      end

      def set(path : String, hash : Bytes, size : Int64) : Nil
        @checksums[path] = Entry.new(hash, size)
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
