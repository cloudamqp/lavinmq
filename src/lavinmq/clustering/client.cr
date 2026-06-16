require "../data_dir_lock"
require "../clustering"
require "../rate_limiter"
require "./checksums"
require "./metadata"
require "./proxy"
require "lz4"
require "http/server"
require "wait_group"

module LavinMQ
  module Clustering
    class Client
      Log = LavinMQ::Log.for "clustering.client"

      # Buffer used when streaming replicated file changes to disk. Matches
      # LZ4::Reader's internal 64 KiB buffer.
      BUFFER_SIZE = 64 * 1024

      # Capacity of the channel buffering acks from the stream-reading fiber to
      # the ack-sending fiber. Only bounds an in-process queue (send_ack_loop
      # drains and coalesces it continuously), so a fixed size is fine; the
      # leader's ack deadline, not this, governs how far a follower may lag.
      ACK_BUFFER_CAPACITY = 8192

      @data_dir_lock : DataDirLock
      @closed = false
      @amqp_proxy : Proxy?
      @http_proxy : Proxy?
      @mqtt_proxy : Proxy?
      @unix_amqp_proxy : Proxy?
      @unix_http_proxy : Proxy?
      @unix_mqtt_proxy : Proxy?
      @socket : TCPSocket?
      @internal_http_server : ::HTTP::Server?
      @streamed_bytes = 0_u64
      @file_digests = Hash(String, Digest::SHA1).new
      @follower_done = Channel(Nil).new
      @connected = Atomic(Int32).new(0)
      # Buffers acks from the stream-reading fiber to the ack-sending fiber.
      # Replaced with a fresh channel on each (re)connect in #stream_changes.
      @acks = Channel(Int64).new
      # Tracks the ack-sending fiber: #close must wait for it to finish before
      # closing @data_dir_fd, since it may sync (syncfs on that fd) before acks
      # it sends — even acks still buffered in @acks after the stream ends.
      @ack_loops = WaitGroup.new

      def initialize(@config : Config, @id : Int32, @password : String, proxy = true)
        System.maximize_fd_limit
        @data_dir = config.data_dir
        @files = Hash(String, File).new do |h, k|
          path = File.join(@data_dir, k)
          Dir.mkdir_p File.dirname(path)
          h[k] = File.open(path, "a").tap &.sync = true
        end
        Dir.mkdir_p @data_dir
        @data_dir_fd = LibC.open(@data_dir.check_no_null_byte, LibC::O_RDONLY)
        raise IO::Error.from_errno("Failed to open #{@data_dir}") if @data_dir_fd < 0
        @data_dir_lock = DataDirLock.new(@data_dir).tap &.acquire
        backup_dir = File.join(@data_dir, "backups")
        FileUtils.rm_rf(backup_dir) if Dir.exists?(backup_dir)
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
        start_metrics_server unless @config.metrics_http_port == -1
        @internal_http_server = HTTP::Server.follower_internal_socket_http_server
      end

      private def start_metrics_server
        @metrics_server = metrics_server = LavinMQ::HTTP::MetricsServer.new
        metrics_server.bind_tcp(@config.metrics_http_bind, @config.metrics_http_port)
        spawn(name: "HTTP metrics listener") do
          metrics_server.listen
        end
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
          @connected.set(1)
          socket.sync = true
          socket.read_buffering = false # use lz4 buffering
          lz4 = Compress::LZ4::Reader.new(socket)
          sync(socket, lz4)
          Log.info { "Streaming changes" }
          stream_changes(socket, lz4)
        rescue ex : IO::Error
          @connected.set(0)
          lz4.try &.close
          socket.try &.close
          break if @closed
          Log.info { "Disconnected from server #{host}:#{port} (#{ex}), retrying..." }
          sleep 1.seconds
        end
      ensure
        @connected.set(0)
        @follower_done.send(nil)
      end

      def connected? : Bool
        @connected.get == 1
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
        files_to_delete, dirs_to_delete = ls_r(@data_dir)
        requested_files = Array(String).new
        file_count = 0
        Log.info { "Calculating checksums and comparing files" }
        log_limiter = RateLimiter.new(2.seconds)
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = lz4.read_string(filename_len)
          lz4.read_fully(remote_hash)
          path = File.join(@data_dir, filename)
          files_to_delete.delete(path)
          # Walk up the path to remove all ancestors from dirs_to_delete
          dir = File.dirname(path)
          while dirs_to_delete.delete(dir)
            dir = File.dirname(dir)
          end
          if File.exists? path
            unless local_hash = @checksums[filename]?
              Log.debug { "Calculating checksum for #{filename}" }
              sha1.file(path)
              local_hash = sha1.final
              @checksums[filename] = local_hash
              sha1.reset
              Fiber.yield # CPU bound, so allow other fibers to run
            end
            if local_hash != remote_hash
              Log.info { "Mismatching hash: #{path}" }
              File.delete path
              requested_files << filename
              request_file(filename, socket)
            else
              Log.debug { "Matching hash: #{path}" }
            end
          else
            requested_files << filename
            request_file(filename, socket)
          end
          file_count &+= 1
          log_limiter.do { Log.info { "Compared #{file_count} files" } }
        end
        end_of_file_list(socket)
        Log.info { "Compared #{file_count} files, #{requested_files.size} to sync" }
        Log.info { "Deleting #{files_to_delete.size} files not on leader" } unless files_to_delete.empty?
        files_to_delete.each do |path|
          Log.debug { "File not on leader: #{path}" }
          File.delete path
        rescue ex : File::Error
          Log.warn(exception: ex) { "Failed to delete #{path}" }
        end
        # Clean up any local empty directory
        # Sort and reverse to cleanup longer paths first
        Log.info { "Deleting #{dirs_to_delete.size} directories not on leader" } unless dirs_to_delete.empty?
        dirs_to_delete.sort!.reverse_each do |path|
          if Dir.empty? path
            Log.debug { "Dir empty or missing on leader: #{path}" }
            Dir.delete? path
          else
            Log.warn { "Dir #{path} in delete set, but not empty?" }
          end
        rescue ex : File::Error
          Log.warn(exception: ex) { "Failed to delete #{path}" }
        end
        received_count = 0
        log_limiter = RateLimiter.new(2.seconds)
        requested_files.each do |filename|
          file_from_socket(filename, lz4)
          received_count &+= 1
          log_limiter.do { Log.info { "Received #{received_count}/#{requested_files.size} files" } }
        end
        Log.info { "Received all #{requested_files.size} files" } unless requested_files.empty?
      end

      private def ls_r(dir) : {Array(String), Array(String)}
        files = Array(String).new
        dirs = Array(String).new
        ls_r(dir) do |filename|
          if File.file?(filename)
            files << filename
          elsif File.directory?(filename)
            dirs << filename
          end
        end
        {files, dirs}
      end

      private def ls_r(dir, &blk : String -> Nil)
        Dir.each_child(dir) do |child|
          path = File.join(dir, child)
          if File.directory? path
            yield path
            ls_r(path, &blk)
          else
            next if Clustering.metadata_file?(child)
            yield path
          end
        end
      end

      private def request_file(filename, socket)
        Log.debug { "Requesting #{filename}" }
        socket.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
        socket.write filename.to_slice
      end

      private def end_of_file_list(socket)
        socket.write_bytes 0 # endian-agnostic
      end

      private def file_from_socket(filename, lz4)
        Log.debug { "Waiting for #{filename}" }
        path = File.join(@data_dir, filename)
        Dir.mkdir_p File.dirname(path)
        length = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        Log.debug { "Receiving #{filename}, #{length.humanize_bytes}" }
        File.open(path, "w") do |f|
          buffer = uninitialized UInt8[BUFFER_SIZE]
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
        Log.debug { "Received #{filename}, #{length.humanize_bytes}" }
      end

      private def stream_changes(socket, lz4)
        acks = @acks = Channel(Int64).new(ACK_BUFFER_CAPACITY)
        @ack_loops.spawn(name: "Send ack loop") { send_ack_loop(acks, socket) }
        spawn log_streamed_bytes_loop, name: "Log streamed bytes loop"
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          next if filename_len.zero?
          filename = lz4.read_string(filename_len)

          len = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          # For append/replace the framing bytes (length headers + filename)
          # are acked up front and the payload is acked incrementally as it's
          # written (see stream_with_checksum), so a single large action keeps
          # the leader's progress deadline reset instead of going silent until
          # it's done. For a delete the framing is the entire record — acking
          # it tells the leader the deletion is durable — so it's only acked
          # once the deletion has been applied.
          framing = sizeof(Int32) + filename_len + sizeof(Int64)
          case len
          when .negative? # append bytes to file
            ack(framing)
            append(filename, len, lz4)
          when .zero? # file is deleted
            delete(filename)
            ack(framing)
          when .positive? # replace file
            ack(framing)
            replace(filename, len, lz4)
          end
        end
      ensure
        @acks.close
      end

      private def append(filename, len, lz4)
        Log.debug { "Appending #{len.abs} bytes to #{filename}" }
        f = @files[filename]
        stream_with_checksum(filename, lz4, f, len.abs)
      end

      private def delete(filename)
        Log.debug { "Deleting #{filename}" }
        if f = @files.delete(filename)
          f.delete
          f.close
        else
          File.delete? File.join(@data_dir, filename)
        end
        @checksums.delete(filename)
        @file_digests.delete(filename)
        delete_empty_dirs File.dirname(filename)
      end

      # Removes now-empty parent directories (e.g. an emptied queue dir) after a
      # file delete. The leader only streams file deletes, not directory deletes,
      # so without this empty queue dirs would linger until the next full sync.
      #
      # We walk up one level per iteration until File.dirname reaches ".". The
      # non-recursive Dir.delete raises File::Error if the dir still has files
      # (or is already gone), and the rescue stops the walk safely. Both append
      # and replace file re-create the full path if needed.
      private def delete_empty_dirs(dir)
        while dir != "."
          path = File.join(@data_dir, dir)
          rmdir(path) || break
          Log.debug { "Deleted empty dir #{dir}" }
          dir = File.dirname(dir)
        end
      rescue ex : File::Error
        Log.error(exception: ex) { "Could not delete #{dir}: #{ex.message}" }
      end

      # rmdir returns false if the dir isn't empty, true if it was removed, and raises on other errors (e.g. permissions).
      private def rmdir(path)
        if LibC.rmdir(path.check_no_null_byte) == 0
          true
        elsif Errno.value.in?(Errno::ENOTEMPTY, Errno::EEXIST, Errno::ENOENT)
          false
        else
          raise ::File::Error.from_errno("Unable to remove directory", file: path)
        end
      end

      private def replace(filename, len, lz4)
        Log.debug { "Replacing file #{filename} (#{len} bytes)" }
        @files.delete(filename).try &.close

        # Reset SHA1 digest for replaced file
        @file_digests[filename] = Digest::SHA1.new

        path = File.join(@data_dir, "#{filename}.tmp")
        Dir.mkdir_p File.dirname(path)
        File.open(path, "w") do |f|
          f.sync = true
          # The record's final ack tells the leader the replace is durable, so
          # it must not be sent while the new content only exists as the .tmp
          # file; hold it back until the rename has installed the file.
          deferred = stream_with_checksum(filename, lz4, f, len, defer_final_ack: true)
          f.rename f.path[0..-5]
          ack(deferred)
        end
      end

      # Read from lz4, update SHA1, and write to file incrementally.
      # Returns the number of bytes received but not yet acked (see below).
      private def stream_with_checksum(filename : String, lz4 : IO, file : IO, length : Int64, defer_final_ack = false) : Int64
        # Get or create SHA1 digest for this file
        sha1 = @file_digests[filename] ||= Digest::SHA1.new

        # Read, hash, and write incrementally. Each chunk is acked as soon as
        # it's persisted so the leader sees continuous progress within a large
        # action and won't evict us on its ack deadline (a 128 MiB message would
        # otherwise stream for >10s with no ack on a 100 Mbit/s link).
        # With defer_final_ack the last chunk is not acked but its size
        # returned, for callers that must apply the action (replace's rename)
        # before the leader may consider it durable.
        buffer = uninitialized UInt8[BUFFER_SIZE]
        remaining = length
        while remaining > 0
          len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, remaining)])
          raise IO::EOFError.new if len.zero?
          bytes = buffer.to_slice[0, len]
          file.write(bytes)
          sha1.update(bytes)
          remaining -= len
          return len.to_i64 if remaining.zero? && defer_final_ack
          ack(len)
        end
        0i64
      end

      # Count streamed bytes and forward the count to the ack-sending fiber.
      private def ack(bytes : Int) : Nil
        n = bytes.to_i64
        @streamed_bytes &+= n
        @acks.send(n)
      end

      # Concatenate as many acks as possible to generate few TCP packets.
      # Data is synced to disk before each ack is sent unless sync is disabled:
      # the leader holds publish confirms until in-sync followers have acked,
      # so an acked byte must be durable here in normal operation. Syncing once
      # per coalesced batch makes batching emerge naturally — acks accumulate
      # while the blocking syncfs runs.
      private def send_ack_loop(acks, socket)
        socket.tcp_nodelay = true
        while ack_bytes = acks.receive?
          while ack_bytes2 = acks.try_receive?
            ack_bytes += ack_bytes2
          end
          sync_to_disk
          socket.write_bytes ack_bytes, IO::ByteFormat::LittleEndian # ack
        end
      rescue Channel::ClosedError
      rescue IO::Error
        socket.close rescue nil
      end

      # Make all replicated writes durable before acking the leader.
      private def sync_to_disk : Nil
        return unless @config.sync?

        sync_data_dir
      rescue ex
        # Can't ack data that isn't durable; die fast so the leader drops us
        # from the in-sync set and stops confirming publishes on our acks.
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end

      private def sync_data_dir : Nil
        {% if flag?(:linux) %}
          ret = LibC.syncfs(@data_dir_fd)
          raise IO::Error.from_errno("syncfs") if ret != 0
        {% else %}
          LibC.sync
        {% end %}
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
        return if @closed
        @closed = true
        @internal_http_server.try &.close
        @amqp_proxy.try &.close
        @http_proxy.try &.close
        @mqtt_proxy.try &.close
        @unix_amqp_proxy.try &.close
        @unix_http_proxy.try &.close
        @unix_mqtt_proxy.try &.close
        @files.each_value &.close
        @socket.try &.close
        # Wait for follower loop to exit (with timeout to prevent hanging)
        select
        when @follower_done.receive
        when timeout(5.seconds)
          Log.warn { "Follower loop did not exit within timeout, forcing shutdown" }
        end
        # The ack loop keeps draining acks buffered in @acks even after the
        # channel is closed, syncing to disk before each send. Wait for it to
        # finish before closing @data_dir_fd below, or its syncfs would hit a
        # closed (or worse, reused) fd and the process would exit 1 mid
        # shutdown/promotion. Closing @acks is normally done by stream_changes,
        # but do it here too in case the follower loop is stuck.
        @acks.close
        @ack_loops.wait
        # Finalize all pending checksums
        @file_digests.each do |filename, sha1|
          @checksums[filename] = sha1.final
        end
        @file_digests.clear
        @checksums.store
        LibC.close(@data_dir_fd) if @data_dir_fd >= 0
        @data_dir_lock.release
        @metrics_server.try &.close
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
