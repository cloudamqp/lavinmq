require "../data_dir_lock"
require "../clustering"
require "./checksums"
require "./proxy"
require "lz4"

module LavinMQ
  module Clustering
    class Client
      Log = LavinMQ::Log.for "clustering.client"
      @data_dir_lock : DataDirLock
      @closed = false
      @amqp_proxy : Proxy?
      @http_proxy : Proxy?
      @mqtt_proxy : Proxy?
      @unix_amqp_proxy : Proxy?
      @unix_http_proxy : Proxy?
      @unix_mqtt_proxy : Proxy?
      @socket : TCPSocket?
      @streamed_bytes = 0_u64

      def initialize(@config : Config, @id : Int32, @password : String, proxy = true)
        System.maximize_fd_limit
        @data_dir = config.data_dir
        @files = Hash(String, File).new do |h, k|
          path = File.join(@data_dir, k)
          Dir.mkdir_p File.dirname(path)
          h[k] = File.open(path, "a").tap &.sync = true
        end
        Dir.mkdir_p @data_dir
        @data_dir_lock = DataDirLock.new(@data_dir).tap &.acquire
        @backup_dir = File.join(@data_dir, "backups", Time.utc.to_rfc3339)
        @checksums = Checksums.new(@data_dir)
        @checksums.restore

        if proxy
          @amqp_proxy = Proxy.new(@config.amqp_bind, @config.amqp_port)
          @http_proxy = Proxy.new(@config.http_bind, @config.http_port)
          @mqtt_proxy = Proxy.new(@config.mqtt_bind, @config.mqtt_port)
          @unix_amqp_proxy = Proxy.new(@config.unix_path) unless @config.unix_path.empty?
          @unix_http_proxy = Proxy.new(@config.http_unix_path) unless @config.http_unix_path.empty?
          @unix_mqtt_proxy = Proxy.new(@config.mqtt_unix_path) unless @config.mqtt_unix_path.empty?
        end
        HTTP::Server.follower_internal_socket_http_server
      end

      def follow(uri : String)
        follow(URI.parse(uri))
      end

      def follow(uri : URI)
        host = uri.hostname.not_nil!("Host missing in follow URI")
        port = uri.port || 5679
        follow(host, port)
      end

      def follow(host : String, port : Int32)
        Log.info { "Following #{host}:#{port}" }
        @host = host
        @port = port
        if amqp_proxy = @amqp_proxy
          spawn amqp_proxy.forward_to(host, @config.amqp_port, true), name: "AMQP proxy"
        end
        if http_proxy = @http_proxy
          spawn http_proxy.forward_to(host, @config.http_port), name: "HTTP proxy"
        end
        if mqtt_proxy = @mqtt_proxy
          spawn mqtt_proxy.forward_to(host, @config.mqtt_port), name: "MQTT proxy"
        end
        if unix_amqp_proxy = @unix_amqp_proxy
          spawn unix_amqp_proxy.forward_to(host, @config.amqp_port), name: "AMQP proxy"
        end
        if unix_http_proxy = @unix_http_proxy
          spawn unix_http_proxy.forward_to(host, @config.http_port), name: "HTTP proxy"
        end
        if unix_mqtt_proxy = @unix_mqtt_proxy
          spawn unix_mqtt_proxy.forward_to(host, @config.mqtt_port), name: "MQTT proxy"
        end
        loop do
          @socket = socket = TCPSocket.new(host, port)
          socket.sync = true
          socket.read_buffering = false
          lz4 = Compress::LZ4::Reader.new(socket)
          sync(socket, lz4)
          Log.info { "Streaming changes" }
          stream_changes(socket, lz4)
        rescue ex : IO::Error
          lz4.try &.close
          socket.try &.close
          break if @closed
          Log.info { "Disconnected from server #{host}:#{port} (#{ex}), retrying..." }
          sleep 1.seconds
        end
      end

      def follows?(_nil : Nil) : Bool
        false
      end

      def follows?(uri : String) : Bool
        follows? URI.parse(uri)
      end

      def follows?(uri : URI) : Bool
        host = uri.hostname.not_nil!("Host missing in follow URI")
        port = uri.port || 5679
        follows?(host, port)
      end

      def follows?(host : String, port : Int32) : Bool
        @host == host && @port == port
      end

      private def sync(socket, lz4)
        Log.info { "Connected" }
        authenticate(socket)
        Log.info { "Authenticated" }
        set_socket_opts(socket)
        sync_files(socket, lz4)
        Log.info { "Bulk synchronised" }
        sync_files(socket, lz4)
        Log.info { "Fully synchronised" }
      end

      private def set_socket_opts(socket)
        if keepalive = @config.tcp_keepalive
          socket.keepalive = true
          socket.tcp_keepalive_idle = keepalive[0]
          socket.tcp_keepalive_interval = keepalive[1]
          socket.tcp_keepalive_count = keepalive[2]
        end
      end

      private def sync_files(socket, lz4)
        Log.info { "Waiting for list of files" }
        sha1 = Digest::SHA1.new
        remote_hash = Bytes.new(sha1.digest_size)
        files_to_delete = ls_r(@data_dir)
        requested_files = Array(String).new
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = lz4.read_string(filename_len)
          lz4.read_fully(remote_hash)
          path = File.join(@data_dir, filename)
          files_to_delete.delete(path)
          if File.exists? path
            unless local_hash = @checksums[filename]?
              Log.info { "Calculating checksum for #{filename}" }
              sha1.file(path)
              local_hash = sha1.final
              @checksums[filename] = local_hash
              sha1.reset
              Fiber.yield # CPU bound, so allow other fibers to run
            end
            if local_hash != remote_hash
              Log.info { "Mismatching hash: #{path}" }
              move_to_backup path
              requested_files << filename
              request_file(filename, socket)
            end
          else
            requested_files << filename
            request_file(filename, socket)
          end
        end
        end_of_file_list(socket)
        Log.info { "List of files received" }
        files_to_delete.each do |path|
          Log.info { "File not on leader: #{path}" }
          move_to_backup path
        end
        requested_files.each do |filename|
          file_from_socket(filename, lz4)
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
            next if child.in?(".lock", ".clustering_id")
            yield path
          end
        end
      end

      private def request_file(filename, socket)
        Log.info { "Requesting #{filename}" }
        socket.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
        socket.write filename.to_slice
      end

      private def end_of_file_list(socket)
        socket.write_bytes 0, IO::ByteFormat::LittleEndian
      end

      private def file_from_socket(filename, lz4)
        path = File.join(@data_dir, filename)
        Dir.mkdir_p File.dirname(path)
        length = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        File.open(path, "w") do |f|
          buffer = uninitialized UInt8[65536]
          remaining = length
          sha1 = Digest::SHA1.new
          while (len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, Math.max(remaining, 0))])) > 0
            bytes = buffer.to_slice[0, len]
            f.write bytes
            sha1.update bytes
            remaining &-= len
          end
          remaining.zero? || raise IO::EOFError.new
          @checksums[filename] = sha1.final
        end
        Log.info { "Received #{filename}, #{length.humanize_bytes}" }
      end

      private def stream_changes(socket, lz4)
        acks = Channel(Int64).new(@config.clustering_max_unsynced_actions)
        spawn send_ack_loop(acks, socket), name: "Send ack loop"
        spawn log_streamed_bytes_loop, name: "Log streamed bytes loop"
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          next if filename_len.zero?
          filename = lz4.read_string(filename_len)

          len = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          case len
          when .negative? # append bytes to file
            append(filename, len, lz4)
          when .zero? # file is deleted
            delete(filename)
          when .positive? # replace file
            replace(filename, len, lz4)
          end
          ack_bytes = len.abs + sizeof(Int64) + filename_len + sizeof(Int32)
          @streamed_bytes &+= ack_bytes
          acks.send(ack_bytes)
        end
      ensure
        acks.try &.close
      end

      private def append(filename, len, lz4)
        Log.debug { "Appending #{len.abs} bytes to #{filename}" }
        f = @files[filename]
        IO.copy(lz4, f, len.abs) == len.abs || raise IO::EOFError.new("Full append not received")
      end

      private def delete(filename)
        Log.debug { "Deleting #{filename}" }
        if f = @files.delete(filename)
          f.delete
          f.close
        else
          File.delete? File.join(@data_dir, filename)
        end
      end

      private def replace(filename, len, lz4)
        Log.debug { "Replacing file #{filename} (#{len} bytes)" }
        f = @files["#{filename}.tmp"]
        IO.copy(lz4, f, len) == len || raise IO::EOFError.new("Full file not received")
        f.rename f.path[0..-5]
        @files.delete("#{filename}.tmp").try &.close
      end

      # Concatenate as many acks as possible to generate few TCP packets
      private def send_ack_loop(acks, socket)
        socket.tcp_nodelay = true
        while ack_bytes = acks.receive?
          while ack_bytes2 = acks.try_receive?
            ack_bytes += ack_bytes2
          end
          socket.write_bytes ack_bytes, IO::ByteFormat::LittleEndian # ack
        end
      rescue Channel::ClosedError
      rescue IO::Error
        socket.close rescue nil
      end

      private def log_streamed_bytes_loop
        loop do
          sleep 30.seconds
          break if @closed
          Log.info { "Total streamed bytes: #{@streamed_bytes}" }
        end
      end

      private def authenticate(socket)
        socket.write Start
        socket.write_bytes @password.bytesize.to_u8, IO::ByteFormat::LittleEndian
        socket.write @password.to_slice
        case socket.read_byte
        when 0 # ok
        when 1   then raise AuthenticationError.new
        when nil then raise IO::EOFError.new
        else
          raise Error.new("Unknown response from authentication")
        end
        socket.write_bytes @id, IO::ByteFormat::LittleEndian
      end

      def close
        @closed = true
        @amqp_proxy.try &.close
        @http_proxy.try &.close
        @mqtt_proxy.try &.close
        @unix_amqp_proxy.try &.close
        @unix_http_proxy.try &.close
        @unix_mqtt_proxy.try &.close
        @files.each_value &.close
        @checksums.store
        @data_dir_lock.release
        @socket.try &.close
      end

      class Error < Exception; end

      class AuthenticationError < Error
        def initialize
          super("Authentication error")
        end
      end
    end
  end
end
