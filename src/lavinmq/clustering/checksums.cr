module LavinMQ
  module Clustering
    class Checksums
      Log = LavinMQ::Log.for "clustering.checksums"
      @checksums = Hash(String, Bytes).new
      @lock = Mutex.new

      def initialize(@data_dir : String)
      end

      def store : Nil
        Dir.mkdir_p(@data_dir)
        File.open(File.join(@data_dir, "checksums.sha1"), "w") do |f|
          @lock.synchronize do
            @checksums.each do |path, hash|
              f.puts "#{hash.hexstring} *#{path}"
            end
          end
        end
        Log.info { "Wrote #{self.size} checksums to disk" }
      end

      def restore : Nil
        File.open(File.join(@data_dir, "checksums.sha1")) do |f|
          @lock.synchronize do
            loop do
              hash = f.read_string(40).hexbytes
              f.skip(2) # " *"
              path = f.read_line
              @checksums[path] = hash
            rescue IO::EOFError
              break
            end
          end
          f.delete # prevent out-of-date hashes to be restored in the event of a crash
          Log.info { "Restored #{self.size} checksums from disk" }
        end
      rescue File::NotFoundError
        Log.info { "Checksums not found" }
      end

      def []?(path)
        @lock.synchronize { @checksums[path]? }
      end

      def []=(path, value)
        @lock.synchronize { @checksums[path] = value }
      end

      def delete(path)
        @lock.synchronize { @checksums.delete(path) }
      end

      def clear
        @lock.synchronize { @checksums.clear }
      end

      def size
        @lock.synchronize { @checksums.size }
      end
    end
  end
end
