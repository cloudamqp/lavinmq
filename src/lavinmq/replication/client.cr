require "../replication"
require "../data_dir_lock"
require "zstd/decompress/io"

module LavinMQ
  class Replication
    class Client
      Log = ::Log.for("replication")
      @password : String
      @data_dir_lock : DataDirLock?
      @closed = false

      def initialize(@data_dir : String)
        @socket = TCPSocket.new
        @zstd = Zstd::Decompress::IO.new(@socket)
        @password = password
        @files = Hash(String, File).new do |h, k|
          path = File.join(@data_dir, k)
          Dir.mkdir_p File.dirname(path)
          h[k] = File.open(path, "a").tap &.sync = true
        end
        Dir.mkdir_p @data_dir
        @data_dir_lock = DataDirLock.new(@data_dir).tap &.acquire
        @backup_dir = File.join(@data_dir, "backups", Time.utc.to_rfc3339)
      end

      private def password : String
        path = File.join(@data_dir, ".replication_secret")
        if File.info(path).permissions.value != 0o400
          raise ArgumentError.new("File permissions of #{path} has to be 0400")
        end
        File.read(path).chomp
      rescue File::NotFoundError
        raise ArgumentError.new("#{path} is missing")
      end

      def follow(uri : URI)
        host = uri.host.not_nil!("Host missing in follow URI")
        port = uri.port || 5679
        follow(host, port)
      end

      def follow(host, port)
        loop do
          @socket.connect(host, port)
          Log.info { "Connected" }
          @socket.write Start
          authenticate
          Log.info { "Authenticated" }
          sync_files
          Log.info { "Files synced" }
          notify_in_sync
          Log.info { "Streaming changes" }
          stream_changes
        rescue ex : IO::Error
          @zstd.close
          @socket.close
          break if @closed
          Log.info { "Disconnected from server (#{ex}), retrying..." }
          sleep 5
          @socket = TCPSocket.new
          @zstd = Zstd::Decompress::IO.new(@socket)
        end
      end

      def sync(host, port)
        @socket.connect(host, port)
        Log.info { "Connected" }
        @socket.write Start
        authenticate
        Log.info { "Authenticated" }
        sync_files
        Log.info { "Synchronised" }
      end

      private def sync_files
        sha1 = Digest::SHA1.new
        remote_hash = Bytes.new(sha1.digest_size)
        local_hash = Bytes.new(sha1.digest_size)
        files_to_delete = ls_r(@data_dir)
        missing_files = Array(String).new
        loop do
          filename_len = @zstd.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = @zstd.read_string(filename_len)
          @zstd.read_fully(remote_hash)
          path = File.join(@data_dir, filename)
          files_to_delete.delete(path)
          if File.exists? path
            sha1.file(path)
            sha1.final(local_hash)
            sha1.reset
            if local_hash != remote_hash
              Log.info { "Mismatching hash: #{path}" }
              move_to_backup path
              missing_files << filename
            end
          else
            missing_files << filename
          end
        end
        files_to_delete.each do |path|
          Log.info { "File not on leader: #{path}" }
          move_to_backup path
        end
        missing_files.each do |filename|
          request_file(filename)
          file_from_socket(filename)
        end
      end

      private def move_to_backup(path)
        backup_path = path.sub(@data_dir, @backup_dir)
        Dir.mkdir_p File.dirname(backup_path)
        File.rename path, backup_path
      end

      private def ls_r(dir) : Array(String)
        files = Array(String).new
        ls_r(dir) do |filename|
          files << filename
        end
        files
      end

      private def ls_r(dir, &blk : String -> Nil)
        Dir.each_child(dir) do |child|
          path = File.join(dir, child)
          if File.directory? path
            next if child.in?("backups")
            ls_r(path, &blk)
          else
            next if child.in?(".lock", ".replication_secret")
            yield path
          end
        end
      end

      private def notify_in_sync
        @socket.write_bytes 0i32, IO::ByteFormat::LittleEndian
      end

      private def request_file(filename)
        Log.info { "Requesting #{filename}" }
        @socket.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
        @socket.write filename.to_slice
      end

      private def file_from_socket(filename)
        path = File.join(@data_dir, filename)
        Dir.mkdir_p File.dirname(path)
        length = @zstd.read_bytes Int64, IO::ByteFormat::LittleEndian
        File.open(path, "w") do |f|
          copied = IO.copy(@zstd, f, length)
          raise IO::Error.new("Only received #{copied}/#{length}") if copied != length
        end
      end

      private def stream_changes(socket = @zstd)
        loop do
          filename_len = socket.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?
          filename = socket.read_string(filename_len)

          len = socket.read_bytes Int64, IO::ByteFormat::LittleEndian
          case len
          when .negative? # append bytes to file
            Log.debug { "Appending #{len.abs} bytes to #{filename}" }
            f = @files[filename]
            copied = IO.copy(socket, f, len.abs)
            raise IO::Error.new("Only received #{copied}/#{len.abs}") if copied != len.abs
          when .zero? # file is deleted
            Log.info { "Deleting #{filename}" }
            if f = @files.delete(filename)
              f.delete
              f.close
            else
              File.delete? File.join(@data_dir, filename)
            end
          when .positive? # full file is coming
            Log.info { "Getting full file #{filename}" }
            f = @files[filename]
            f.truncate
            copied = IO.copy(socket, f, len)
            raise IO::Error.new("Only received #{copied}/#{len}") if copied != len
          end
          @socket.write_bytes len.abs, IO::ByteFormat::LittleEndian # ack
        end
      end

      private def authenticate
        @socket.write_bytes @password.bytesize.to_u8, IO::ByteFormat::LittleEndian
        @socket.write @password.to_slice
      end

      def close
        @closed = true
        @zstd.close
        @socket.close
      end
    end
  end
end
