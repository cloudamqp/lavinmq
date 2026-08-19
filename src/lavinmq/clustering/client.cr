require "../data_dir_lock"
require "../clustering"
require "../rate_limiter"
require "./checksums"
require "./control_packet"
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

      # Files #hash_local_files hashes between Fiber.yields; a yield per (often
      # tiny) file costs more than it gives back.
      HASH_YIELD_INTERVAL = 32

      # Capacity of the channel buffering acks from the stream-reading fiber to
      # the ack-sending fiber. Only bounds an in-process queue (send_ack_loop
      # drains and coalesces it continuously), so a fixed size is fine; the
      # leader's ack deadline, not this, governs how far a follower may lag.
      ACK_BUFFER_CAPACITY = 8192

      @data_dir_lock : DataDirLock
      @closed = Atomic(Bool).new(false)
      @amqp_proxy : Proxy?
      @http_proxy : Proxy?
      @mqtt_proxy : Proxy?
      @unix_amqp_proxy : Proxy?
      @unix_http_proxy : Proxy?
      @unix_mqtt_proxy : Proxy?
      @socket : TCPSocket?
      @internal_http_server : ::HTTP::Server?
      @streamed_bytes = 0_u64
      # syncfs(2) calls made
      @syncfs_calls = 0_u64
      # Running SHA1 over each file's whole content, adopted as its checksum when
      # tracking ends. nil when we started seeing the file mid-content, so no
      # digest can cover the bytes already on disk (see #digest_for).
      @file_digests = Hash(String, Digest::SHA1?).new
      @follower_done = Channel(Nil).new
      # Buffers acks from the stream-reading fiber to the ack-sending fiber.
      # Replaced with a fresh channel on each (re)connect in #stream_changes.
      @acks = Channel(Int64).new
      # Tracks the ack-sending fiber, so #close lets it drain the acks still
      # buffered in @acks after the stream ends.
      @ack_loops = WaitGroup.new
      # Tracks control actions in flight, so #close waits for one to finish
      # before tearing down the state it touches (see #control).
      @controls = WaitGroup.new

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
        @internal_http_server ||= HTTP::Server.follower_internal_socket_http_server unless local_leader_host?(host)
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
          hash_local_files
          return if @closed.get
          @socket = socket = TCPSocket.new(host, port)
          socket.sync = true
          socket.read_buffering = false # use lz4 buffering
          lz4 = Compress::LZ4::Reader.new(socket)
          sync(socket, lz4)
          Log.info { "Streaming changes" }
          stream_changes(socket, lz4)
        rescue ex : IO::Error
          lz4.try &.close
          socket.try &.close
          break if @closed.get
          Log.info { "Disconnected from server #{host}:#{port} (#{ex}), retrying..." }
          sleep 1.seconds
        end
      ensure
        @follower_done.send(nil)
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

      private def local_leader_host?(host : String) : Bool
        host = host.downcase
        return true if host == System.hostname.downcase
        host = host[1...-1] if host.starts_with?("[") && host.ends_with?("]")

        Socket::Addrinfo.tcp(host, 0).any?(&.ip_address.loopback?)
      rescue Socket::Error
        false
      end

      private def sync(socket, lz4)
        Log.info { "Connected" }
        authenticate(socket)
        Log.info { "Authenticated" }
        set_socket_opts(socket)
        full_sync(socket, lz4)
      end

      private def full_sync(socket, lz4)
        reset_file_state
        full_sync_time = Time.measure do
          bulk_time = Time.measure { sync_files(socket, lz4) }
          Log.info { "Bulk synchronised in #{bulk_time.total_seconds} seconds" }
          rest_time = Time.measure { sync_files(socket, lz4) }
          Log.info { "Changes since bulk synchronized in #{rest_time.total_seconds} seconds" }
        end
        Log.info { "Fully synchronised in #{full_sync_time.total_seconds} seconds" }
      end

      # Forget the data dir as the previous connection left it. The sync ahead
      # deletes and re-fetches every file whose hash doesn't match the leader's,
      # which would leave cached handles writing to unlinked inodes and digests
      # covering content that's no longer on disk.
      private def reset_file_state : Nil
        finalize_digests
        @files.each_value &.close
        @files.clear
      end

      # Adopt the running digests as the files' checksums and stop tracking them.
      private def finalize_digests : Nil
        @file_digests.each do |filename, sha1|
          adopt_digest(filename, sha1)
        end
        @file_digests.clear
      end

      # Store `sha1` as `filename`'s checksum, unless it's untracked (nil, i.e.
      # covers only part of the content) or the file is gone.
      private def adopt_digest(filename : String, sha1 : Digest::SHA1?) : Nil
        return unless sha1
        return unless File.exists?(File.join(@data_dir, filename))
        @checksums[filename] = sha1.final
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
        hash_size = Digest::SHA1.new.digest_size

        # Drain the whole file list before hashing anything: stalling between
        # entries can make the leader's file-list write time out and drop us.
        # Once the list is read it waits for our file requests, so the comparison
        # below is free to hash.
        remote_files = Array({String, Bytes}).new
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = lz4.read_string(filename_len)
          remote_hash = Bytes.new(hash_size)
          lz4.read_fully(remote_hash)
          remote_files << {filename, remote_hash}
        end
        Log.info { "Received list of #{remote_files.size} files" }

        # Now compare against local files, with the socket already drained.
        files_to_delete, dirs_to_delete = ls_r(@data_dir)
        requested_files = Array(String).new
        file_count = 0
        files_total = remote_files.size
        Log.info { "Comparing files" }
        log_limiter = RateLimiter.new(2.seconds)
        remote_files.each do |filename, remote_hash|
          path = File.join(@data_dir, filename)
          files_to_delete.delete(path)
          # Walk up the path to remove all ancestors from dirs_to_delete
          dir = File.dirname(path)
          while dirs_to_delete.delete(dir)
            dir = File.dirname(dir)
          end
          if File.exists? path
            # Pre-computed by #hash_local_files, except for files that appeared
            # after that pass.
            unless local_hash = @checksums[filename]?
              local_hash = hash_file(filename, path)
              Fiber.yield # CPU bound, so allow other fibers to run
            end
            if local_hash != remote_hash
              Log.info { "Mismatching hash: #{path}" }
              File.delete path
              requested_files << filename
            else
              Log.debug { "Matching hash: #{path}" }
            end
          else
            requested_files << filename
          end
          file_count &+= 1
          log_limiter.do { Log.info { "Compared #{file_count}/#{files_total} files…" } }
        end
        Log.info { "Compared #{file_count} files, #{requested_files.size} to sync" }
        requested_files.each do |filename|
          request_file(filename, socket)
        end
        end_of_file_list(socket)
        Log.info { "Deleting #{files_to_delete.size} files not on leader" } unless files_to_delete.empty?
        files_to_delete.each do |path|
          Log.debug { "File not on leader: #{path}" }
          File.delete path
          # It got a checksum from #hash_local_files; drop it or the checksum map
          # accumulates dead paths.
          @checksums.delete(relative_path(path))
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

      # Hash every local file before connecting, because hashing while connected
      # holds up the leader, which keeps its sync lock until we answer. Files
      # already in @checksums (from disk or an earlier pass) are skipped.
      private def hash_local_files : Nil
        computed = 0
        files, _dirs = ls_r(@data_dir)
        time = Time.measure do
          Log.info { "Calculating checksums for #{files.size} local files" }
          log_limiter = RateLimiter.new(2.seconds)
          files.each do |path|
            break if @closed.get
            filename = relative_path(path)
            next if @checksums[filename]?
            hash_file(filename, path)
            computed &+= 1
            Fiber.yield if computed % HASH_YIELD_INTERVAL == 0 # CPU bound, so let other fibers run
            log_limiter.do { Log.info { "Calculated #{computed} checksums" } }
          rescue ex : File::NotFoundError
            Log.debug(exception: ex) { "#{path} disappeared while hashing" }
          rescue ex : File::Error
            # This pass also hashes files the leader doesn't have, so one
            # unreadable file must not wedge the reconnect loop. Left uncached,
            # so the compare loop still fails if the leader has it.
            Log.warn(exception: ex) { "Failed to calculate checksum for #{path}" }
          end
          # #restore truncated checksums.sha1, so snapshot the full set or a
          # crash loses the hashes that were on disk at boot.
          @checksums.store if computed > 0
        end
        Log.info { "Calculated #{computed} checksums (#{files.size} local files) in #{time.total_seconds} seconds" }
      end

      # Hash one local file, persisting the hash right away so progress
      # survives a crash.
      private def hash_file(filename : String, path : String) : Bytes
        Log.debug { "Calculating checksum for #{filename}" }
        sha1 = Digest::SHA1.new
        sha1.file(path)
        hash = sha1.final
        @checksums.append(filename, hash)
        hash
      end

      # Path relative to the data dir, i.e. the name the leader knows a file by.
      private def relative_path(path : String) : String
        path.lchop(@data_dir).lchop('/')
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
            # checksums.sha1(.tmp) is local-only replication metadata, never
            # sent by the leader; skip it so the "delete files not on leader"
            # sweep doesn't wipe our persisted hashes mid-sync.
            next if child.in?(".lock", ".clustering_id", "checksums.sha1", "checksums.sha1.tmp")
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
          # Persist immediately too: a file received here is complete and
          # stable, so a crash mid-sync won't force re-hashing it on restart.
          @checksums.append(filename, sha1.final)
        end
        Log.debug { "Received #{filename}, #{length.humanize_bytes}" }
      end

      private def stream_changes(socket, lz4)
        acks = @acks = Channel(Int64).new(ACK_BUFFER_CAPACITY)
        @ack_loops.spawn(name: "Send ack loop") { send_ack_loop(acks, socket) }
        # Stops the logging fiber when this stream ends, so a reconnect doesn't
        # leave one behind per connection (they all report the same counter).
        log_loop_done = Channel(Nil).new
        spawn log_streamed_bytes_loop(log_loop_done), name: "Log streamed bytes loop"
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          next if filename_len.zero?
          filename = lz4.read_string(filename_len)

          len = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          # For append/replace the framing bytes (length headers + filename)
          # are acked up front and the payload is acked incrementally as it's
          # written (see stream_with_checksum), so a single large action keeps
          # the leader's progress deadline reset instead of going silent until
          # it's done. For a delete the framing is the entire record, so it's
          # only acked once the deletion has been applied: an ack may only cover
          # records the follower has already carried out.
          framing = sizeof(Int32) + filename_len + sizeof(Int64)
          # Routed before the length is interpreted: a control record's empty
          # body would read as a delete. Acked once handled, like a delete.
          # Control records carry no body, so the framing is the whole record —
          # and a control record that does carry one is not an instruction this
          # build can act on, so it falls through and is consumed as file data
          # rather than desyncing the stream.
          if len.zero? && ControlPacket.control?(filename)
            control(filename)
            ack(framing)
            next
          end
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
        log_loop_done.try &.close
      end

      # Apply a control record: nothing is read from the stream, written to disk
      # or tracked for its path. An unknown instruction (a newer leader) is
      # ignored rather than fatal.
      #
      # A running action is counted in @controls so #close can't tear down the
      # state it touches. @closed is checked before the count, not inside it:
      # close sets @closed before it reaches @controls.wait, so a false reading
      # means our #add lands before that wait. Counting first would let an #add
      # land after the waiter woke, which makes WaitGroup#wait raise on a
      # positive counter and aborts close halfway.
      #
      # Skipping is safe: close has closed the socket, so the leader dropped us
      # from the ISR and no confirm waits on our ack.
      private def control(command : String) : Nil
        return if @closed.get
        @controls.add
        begin
          case packet = ControlPacket.from_str(command)
          in SyncPacket
            Log.debug { "Sync requested: #{packet.path}" }
            sync_to_disk(packet.path)
          in Nil
            Log.warn { "Ignoring unknown control record #{command}" }
          end
        ensure
          @controls.done
        end
      end

      private def append(filename, len, lz4)
        Log.debug { "Appending #{len.abs} bytes to #{filename}" }
        f = @files[filename]
        stream_with_checksum(lz4, f, len.abs, digest_for(filename, f))
      end

      # The running digest over `filename`'s whole content, or nil if we can't
      # keep one: Digest::SHA1 can't be seeded with a hash, so tracking can only
      # start while the file is empty (`file` is opened in append mode, so its
      # size here is still the pre-append size). If it isn't, the checksum we
      # hold goes stale with this append and is dropped, making the next sync
      # re-hash the file from disk.
      private def digest_for(filename : String, file : File) : Digest::SHA1?
        # #fetch, not #[]?, to tell an absent entry from an untracked file
        @file_digests.fetch(filename) do
          if file.size.zero?
            @file_digests[filename] = Digest::SHA1.new
          else
            Log.debug { "Not checksumming #{filename}, appending to existing content" }
            @checksums.delete(filename)
            @file_digests[filename] = nil
          end
        end
      end

      private def delete(filename)
        Log.debug { "Deleting #{filename}" }
        @files.delete(filename).try &.close
        File.delete? File.join(@data_dir, filename)
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

        # A replace rewrites the file from byte 0, so the digest below covers its
        # whole content — but only once the rename installs it. Until then the old
        # file is on disk, so leave it with a checksum matching it and start
        # tracking the new digest only after the rename.
        adopt_digest(filename, @file_digests.delete(filename))
        sha1 = Digest::SHA1.new

        path = File.join(@data_dir, "#{filename}.tmp")
        Dir.mkdir_p File.dirname(path)
        File.open(path, "w") do |f|
          f.sync = true
          # The record's final ack tells the leader the replace has been applied,
          # so it must not be sent while the new content only exists as the .tmp
          # file; hold it back until the rename has installed the file.
          deferred = stream_with_checksum(lz4, f, len, sha1, defer_final_ack: true)
          f.rename f.path[0..-5]
          @file_digests[filename] = sha1
          ack(deferred)
        end
      end

      # Read from lz4, update SHA1, and write to file incrementally.
      # Returns the number of bytes received but not yet acked (see below).
      # A nil `sha1` streams the bytes without hashing them (see #digest_for).
      private def stream_with_checksum(lz4 : IO, file : IO, length : Int64, sha1 : Digest::SHA1?, defer_final_ack = false) : Int64
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
          sha1.try &.update(bytes)
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
      # Nothing is synced here: an ack means received and applied, and durability
      # is fenced by the leader's sync records (see #control). The
      # Fiber.yield lets a batch grow — the stream-reading fiber gets to queue
      # more acks before we drain the channel again.
      private def send_ack_loop(acks, socket)
        socket.tcp_nodelay = true
        while ack_bytes = acks.receive?
          while ack_bytes2 = acks.try_receive?
            ack_bytes += ack_bytes2
          end
          socket.write_bytes ack_bytes, IO::ByteFormat::LittleEndian # ack
          Fiber.yield
        end
      rescue Channel::ClosedError
      rescue IO::Error
        socket.close rescue nil
      end

      # Make replicated writes durable. Only runs on request: the leader asks
      # with a $ record, acked once this returns, and that ack is what tells it
      # the data is persisted here.
      #
      # A file we have open is fsynced on its own; anything else — a directory,
      # the empty path (the data dir), a file we only ever replaced — falls back
      # to the whole filesystem, which can never sync too little.
      private def sync_to_disk(path : String) : Nil
        if file = @files[path]?
          file.fsync
        else
          sync_data_dir
        end
      rescue ex
        # Can't ack data that isn't durable; die fast so the leader drops us
        # from the in-sync set and stops confirming publishes on our acks.
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end

      private def sync_data_dir : Nil
        @syncfs_calls &+= 1
        {% if flag?(:linux) %}
          ret = LibC.syncfs(@data_dir_fd)
          raise IO::Error.from_errno("syncfs") if ret != 0
        {% else %}
          LibC.sync
        {% end %}
      end

      private def log_streamed_bytes_loop(done : Channel(Nil))
        loop do
          select
          when done.receive?
            break
          when timeout(5.seconds)
            break if @closed.get
            Log.info { stream_stats_message }
          end
        end
      end

      private def stream_stats_message : String
        "Total streamed bytes: #{@streamed_bytes}, syncfs calls: #{@syncfs_calls}"
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
        return if @closed.swap(true)
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
        # Let the ack loop drain the acks buffered in @acks after the stream
        # ended. Closing @acks is normally done by stream_changes, but do it here
        # too in case the follower loop is stuck.
        @acks.close
        @ack_loops.wait
        # Nothing below may run while a control action does: a syncfs would hit a
        # closed (or worse, reused) @data_dir_fd and exit 1 mid promotion. The
        # follower loop's exit above normally covers it; this wait matters when
        # that timed out instead (see #control).
        @controls.wait
        # Finalize all pending checksums
        finalize_digests
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
