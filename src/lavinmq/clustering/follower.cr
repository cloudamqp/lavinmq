require "./file_index"
require "../config"
require "../rate_limiter"
require "socket"
require "wait_group"

module LavinMQ
  module Clustering
    class Follower
      enum State
        Syncing
        Synced
      end
      Log = LavinMQ::Log.for "clustering.follower"

      # Max time a connected follower may go without acking outstanding data
      # before it's disconnected from the replica set. Mirrors the
      # @socket.write_timeout used for the flush path so a slow but
      # still-connected follower can't stall publish confirms indefinitely.
      ACK_TIMEOUT = 3.seconds

      @acked_bytes = Atomic(Int64).new(0)
      @sent_bytes = Atomic(Int64).new(0)
      # Wakes the publish-confirm waiter on each ack; closed (never replaced)
      # when ack_loop ends, which doubles as the follower's death signal (see
      # #dead?).
      @ack_notify = ::Channel(Nil).new(1)
      # Wakes flush_loop; capacity 1 so a burst of requests coalesces into one
      # flush. Closed when ack_loop ends, stopping flush_loop. Carries Bool
      # (not Nil) so receive? distinguishes a request (true) from close (nil).
      @flush_requested = ::Channel(Bool).new(1)
      @write_lock = Mutex.new(:unchecked)
      @running = WaitGroup.new
      @state = State::Syncing
      # Per-file byte offset that this follower already received via full_sync
      # when it was marked synced. Incremental appends below this offset are
      # already in the snapshot and must be skipped to avoid duplicating them.
      @synced_baseline = Hash(String, Int64).new
      getter id = -1
      getter remote_address
      getter state

      def initialize(@socket : TCPSocket, @data_dir : String, @file_index : FileIndex)
        @socket.sync = true # Use buffering in lz4
        @socket.read_buffering = true
        @socket.write_timeout = 3.seconds # don't wait for blocked followers
        @remote_address = @socket.remote_address
        @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
      end

      def negotiate!(password) : Nil
        @socket.read_timeout = 5.seconds # prevent idling non-authed sockets
        validate_header!
        authenticate!(password)
        @id = @socket.read_bytes Int32, IO::ByteFormat::LittleEndian
        if keepalive = Config.instance.tcp_keepalive
          @socket.keepalive = true
          @socket.tcp_keepalive_idle = keepalive[0]
          @socket.tcp_keepalive_interval = keepalive[1]
          @socket.tcp_keepalive_count = keepalive[2]
        end
        @socket.read_timeout = nil # assumed authed followers are well behaving
        Log.info { "Accepted ID #{@id.to_s(36)}" }
      end

      # `caps` (last sync of a joining follower) limits each file to the byte
      # count recorded as that follower's synced baseline, so the snapshot and
      # the baseline agree and in-flight writes aren't duplicated.
      def full_sync(caps : Hash(String, Int64)? = nil) : Nil
        send_file_list(caps: caps)
        send_requested_files(caps: caps)
      end

      def ack_loop(ack_timeout : Time::Span = ACK_TIMEOUT)
        @running.add
        @running.spawn(name: "Clustering follower flush loop") { flush_loop }
        @socket.read_timeout = 100.milliseconds # Wait for an ack max this time, otherwise flush the buffer to trigger acks
        # When data is outstanding and unacked, the time we first noticed it.
        # Reset to nil on any ack (progress) or when fully caught up, so the
        # deadline measures time-since-data-became-outstanding, not since the
        # last ack (which would be stale after an idle period).
        unacked_since : Time::Instant? = nil
        loop do
          begin
            len = @socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
            @acked_bytes.add(len)
            unacked_since = nil       # progress; restart the deadline
            @ack_notify.try_send(nil) # wake any publish-confirm waiter
          rescue IO::TimeoutError
            @write_lock.synchronize do
              @lz4.flush
            end
            # A connected follower that stops acking while data is outstanding
            # (blocked on its own sync, GC pause, half-open socket) would
            # otherwise stall publish confirms indefinitely. Drop it from the
            # replica set like the write_timeout path does for blocked writes;
            # it will re-sync on reconnect. Healthy-but-behind followers keep
            # acking, so unacked_since keeps resetting and they're never dropped.
            if lag_in_bytes > 0
              now = Time.instant
              unacked_since ||= now
              if now - unacked_since > ack_timeout
                Log.warn { "No ack for #{ack_timeout}, disconnecting follower id=#{@id.to_s(36)}" }
                break
              end
            else
              unacked_since = nil
            end
          end
        end
      rescue IO::EOFError | Socket::Error | IO::Error
        # socket closed
      ensure
        @ack_notify.close      # unblock any waiter; this follower is gone
        @flush_requested.close # stop flush_loop
        @running.done
      end

      # True once ack_loop has ended (follower disconnected or timed out) or
      # the follower was closed without ack_loop ever running.
      # Server#update_isr excludes dead followers: a publish-confirm waiter
      # unblocked by the @ack_notify close can flush an ISR without this
      # follower even though it hasn't been removed from @followers yet.
      def dead? : Bool
        @ack_notify.closed?
      end

      # Flush the LZ4 buffer so pending bytes reach the follower without
      # waiting for the ack_loop's 100ms flush timeout. Write errors are
      # swallowed: a broken socket is detected by ack_loop, which closes
      # @ack_notify so a wait_for_confirm waiter still unblocks.
      private def flush : Nil
        @write_lock.synchronize { @lz4.flush }
      rescue IO::Error | Socket::Error
      end

      # Flushes on behalf of request_flush callers. Runs in its own fiber,
      # spawned by ack_loop on the default execution context: the publish
      # confirm loop runs on an isolated thread and must not write the socket
      # itself — the socket's fd belongs to the default context's event loop
      # (ack_loop keeps a read pending on it), and a write that blocks from
      # another context raises instead of waiting.
      private def flush_loop
        while @flush_requested.receive?
          flush
        end
      end

      # Ask flush_loop to push buffered bytes to the follower. Never blocks
      # and never touches the socket, so it's safe to call from any execution
      # context (see flush_loop).
      def request_flush : Nil
        @flush_requested.try_send(true)
      rescue ::Channel::ClosedError
      end

      # Block until the follower has acked at least the bytes already sent at
      # call time. Requests a flush first so the pending bytes reach the
      # follower without waiting for the ack_loop's 100ms flush timeout.
      # Returns true if the bytes were acked, false if the follower
      # disconnected. A follower that stops acking is disconnected by
      # ack_loop, which closes @ack_notify and unblocks us here.
      def wait_for_confirm : Bool
        target = @sent_bytes.get
        request_flush
        until @acked_bytes.get >= target
          @ack_notify.receive
        end
        # Several fibers can wait concurrently (the publish confirm loop and
        # definition fences) but each ack wakes only one; pass the wakeup on
        # so every waiter whose target was reached gets to re-check. A waiter
        # still short of its target swallows the relayed wakeup harmlessly —
        # its missing bytes guarantee another ack (or eviction) follows.
        @ack_notify.try_send(nil)
        true
      rescue ::Channel::ClosedError | IO::Error | Socket::Error
        # follower disconnected; stop waiting for it
        false
      end

      private def validate_header! : Nil
        buf = uninitialized UInt8[8]
        slice = buf.to_slice
        @socket.read_fully(slice)
        if slice != Start
          @socket.write(Start)
          raise InvalidStartHeaderError.new(slice)
        end
      end

      private def authenticate!(password) : Nil
        len = @socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
        client_password = @socket.read_string(len)
        if Crypto::Subtle.constant_time_compare(password, client_password)
          @socket.write_byte 0u8
        else
          @socket.write_byte 1u8
          raise AuthenticationError.new
        end
      end

      private def send_file_list(lz4 = @lz4, caps : Hash(String, Int64)? = nil)
        Log.info { "Calculating hashes for #{@file_index.nr_of_files} files" }
        count = 0
        log_limiter = RateLimiter.new(2.seconds)
        @file_index.files_with_hash(caps) do |path, hash|
          lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
          lz4.write path.to_slice
          lz4.write hash
          count &+= 1
          log_limiter.do { Log.info { "Calculated hash for #{count}/#{@file_index.nr_of_files} files" } }
        end
        lz4.write_bytes 0i32 # 0 means end of file list (endian-agnostic)
        lz4.flush
        Log.info { "File list sent (#{count} files)" }
        count
      end

      private def send_requested_files(socket = @socket, caps : Hash(String, Int64)? = nil)
        requested_files = Array(String).new
        loop do
          filename_len = socket.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?
          filename = socket.read_string(filename_len)
          requested_files << filename
          Log.debug { "#{filename} requested" }
        end
        Log.info { "#{requested_files.size} files requested" }
        total_requested_bytes = requested_files.sum(0i64) do |p|
          @file_index.with_file(p, cap_for(caps, p)) { |_f, size| size }
        end
        sent_bytes = 0i64
        uploaded_count = 0
        log_limiter = RateLimiter.new(2.seconds)
        start = Time.instant
        requested_files.each do |filename|
          file_size = send_requested_file(filename, caps)

          sent_bytes += file_size
          uploaded_count &+= 1
          total_requested_bytes -= file_size
          total_time_taken = (Time.instant - start).total_seconds
          bps = (sent_bytes / total_time_taken).round.to_u64
          time_left = bps > 0 ? (total_requested_bytes / bps).round(1) : 0
          Log.debug { "Uploaded #{filename} at #{bps.humanize_bytes}/s" }
          log_limiter.do { Log.info { "Uploaded #{uploaded_count}/#{requested_files.size} files at #{bps.humanize_bytes}/s, #{total_requested_bytes.humanize_bytes} left (~#{time_left}s)" } }
          Fiber.yield
        end
        Log.info { "Uploaded all #{requested_files.size} files" } unless requested_files.empty?
        @lz4.flush
      end

      # When `caps` is set, a file missing from it is capped at 0 (sent empty,
      # then filled via the change stream); otherwise the file is uncapped.
      private def cap_for(caps : Hash(String, Int64)?, path : String) : Int64?
        return nil unless caps
        caps[path]? || 0i64
      end

      private def send_requested_file(filename, caps : Hash(String, Int64)? = nil) : Int
        @file_index.with_file(filename, cap_for(caps, filename)) do |f, size|
          if f
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            IO.copy(f, @lz4, size) == size || raise IO::EOFError.new
            size
          else
            @lz4.write_bytes 0i64 # missing file marker (endian-agnostic)
            0
          end
        end
      end

      def replace(path) : Int64
        @write_lock.synchronize do
          File.open(File.join(@data_dir, path)) do |file|
            file_size = file.size
            lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + file_size).to_i64
            @sent_bytes.add(lag_size)
            send_filename(path)
            @lz4.write_bytes file_size.to_i64, IO::ByteFormat::LittleEndian
            IO.copy(file, @lz4, file_size) == file_size || raise IO::EOFError.new
            lag_size
          end
        end
      end

      # Replace a file's whole content from an in-memory slice (the leader's
      # mmap, read capped at mfile.size). A positive length on the wire marks a
      # replace; the follower streams it to <path>.tmp then renames atomically.
      # An empty slice writes a 0 length, which the follower applies as a delete
      # (an emptied file carries no data to restore anyway).
      def replace(path : String, bytes : Bytes) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + bytes.bytesize).to_i64
          @sent_bytes.add(lag_size)
          send_filename(path)
          @lz4.write_bytes bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
          @lz4.write bytes
          lag_size
        end
      end

      def append(path : String, bytes : Bytes) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + bytes.bytesize).to_i64
          @sent_bytes.add(lag_size)
          send_filename(path)
          @lz4.write_bytes -bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
          @lz4.write bytes
          lag_size
        end
      end

      def append(path : String, value : UInt32 | Int32) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + 4).to_i64
          @sent_bytes.add(lag_size)
          send_filename(path)
          @lz4.write_bytes -4i64, IO::ByteFormat::LittleEndian
          @lz4.write_bytes value, IO::ByteFormat::LittleEndian
          lag_size
        end
      end

      def delete(path) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64)).to_i64
          @sent_bytes.add(lag_size)
          send_filename(path)
          @lz4.write_bytes 0i64 # delete marker (endian-agnostic)
          lag_size
        end
      end

      private def send_filename(path)
        @lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
        @lz4.write path.to_slice
      end

      def close
        begin
          @write_lock.synchronize do
            @lz4.close
            @socket.close
          end
        rescue IO::Error
          # ignore connection errors while closing
        end
        @running.wait
        # Normally ack_loop's ensure has closed these by now, but a follower
        # whose ack_loop never ran (e.g. its join failed after mark_synced!)
        # must still read as dead and unblock any wait_for_confirm waiter,
        # or a publish confirm could hang on it forever.
        @ack_notify.close
        @flush_requested.close
      end

      def to_json(json : JSON::Builder)
        {
          remote_address:     @remote_address.to_s,
          sent_bytes:         @sent_bytes.get,
          acked_bytes:        @acked_bytes.get,
          lag_in_bytes:       lag_in_bytes,
          compression_ratio:  @lz4.compression_ratio,
          uncompressed_bytes: @lz4.uncompressed_bytes,
          compressed_bytes:   @lz4.compressed_bytes,
          id:                 @id.to_s(36),
        }.to_json(json)
      end

      def lag_in_bytes : Int64
        @sent_bytes.get - @acked_bytes.get
      end

      def syncing?
        @state.syncing?
      end

      def synced?
        @state.synced?
      end

      def mark_synced!
        @state = State::Synced
      end

      # Record, at the moment of becoming synced, how many bytes of each file
      # full_sync just delivered. Keyed by the same (data-dir-relative) path the
      # replication appends use.
      def capture_synced_baseline(sizes : Hash(String, Int64)) : Nil
        @synced_baseline = sizes
      end

      # How many leading bytes of the append `[offset, offset + length)` this
      # follower already received via full_sync (0 = none, `length` = all).
      # The baseline cut is a live file size, so it may land *inside* a record
      # that a writer had written locally but not yet dispatched when the cut
      # was taken; such a straddling append is healed by the caller sending
      # only the tail the snapshot didn't cover (the follower appends raw
      # bytes, it doesn't care about record boundaries).
      #
      # Appends to a given path arrive at monotonically increasing offsets, so
      # the first one ending at or past the cut means the follower has caught
      # up with that file and the entry can be dropped. When the last entry
      # goes the whole baseline is reassigned to a fresh empty hash, releasing
      # the capacity a large join held and turning later checks into a plain
      # miss.
      def already_synced(path : String, offset : Int64, length : Int64) : Int64
        baseline = @synced_baseline[path]?
        return 0i64 unless baseline
        return length if offset + length <= baseline  # entirely in the snapshot
        return baseline - offset if offset < baseline # straddles the cut: has the head
        @synced_baseline.delete(path)                 # caught up with this path
        @synced_baseline = Hash(String, Int64).new if @synced_baseline.empty?
        0i64
      end

      def forget_baseline(path : String) : Nil
        @synced_baseline.delete(path)
      end
    end
  end
end
