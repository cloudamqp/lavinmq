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
      # Op-numbers (the VR viewstamp position) tracked alongside the byte
      # counters. The byte counters drive lag/timeout/observability; the op
      # counters drive the majority-commit quorum gate, because op-numbers are
      # globally comparable across nodes while wire-byte counts are per-connection
      # (they depend on each follower's full_sync cut and skip decisions).
      @acked_op = Atomic(UInt64).new(0)
      @sent_op = Atomic(UInt64).new(0)
      # The op-number at this follower's full_sync cut (set by mark_synced!). Its
      # snapshot is durable only once the follower acks at least this op, so a
      # record fully covered by the snapshot must not count toward the quorum
      # before then. The highest such deferred op is parked here until the
      # baseline ack lands, at which point ack_loop promotes it into @acked_op.
      @synced_baseline_op = Atomic(UInt64).new(0)
      @pending_synced_op = Atomic(UInt64).new(0)
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

      # `commit_notify` is the Server's coalescing wakeup for wait_for_quorum
      # waiters; this follower fires it on each ack and on disconnect so the
      # leader's commit point is re-evaluated. Nil in unit tests with no server.
      def initialize(@socket : TCPSocket, @data_dir : String, @file_index : FileIndex, @commit_notify : ::Channel(Nil)? = nil)
        @socket.sync = true # Use buffering in lz4
        @socket.read_buffering = true
        @socket.write_timeout = 3.seconds # don't wait for blocked followers
        @remote_address = @socket.remote_address
        @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
      end

      def negotiate!(password) : Nil
        @socket.read_timeout = 5.seconds # prevent idling non-authed sockets
        validate_header!
        negotiate_after_header!(password)
      end

      # Negotiate when the 8-byte start header has already been read and
      # validated by the shared listener's router (which peeks it to tell a
      # REPLI data connection from a VRCTL control connection). Does the
      # password + id exchange only.
      def negotiate_after_header!(password) : Nil
        @socket.read_timeout = 5.seconds # prevent idling non-authed sockets
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
      #
      # `baseline_op`, when given (the final capped pass), is the leader's
      # op-number at the cut: the follower now holds everything up to it, so it
      # adopts that as its op high-water (it must NOT keep a stale/empty op after
      # syncing, or it could win a later election and overwrite live data). It's
      # sent last, after the files, and the follower reads it before streaming.
      def full_sync(caps : Hash(String, Int64)? = nil, baseline_op : UInt64? = nil) : Nil
        send_file_list(caps: caps)
        send_requested_files(caps: caps)
        if baseline_op
          @write_lock.synchronize do
            @lz4.write_bytes baseline_op, IO::ByteFormat::LittleEndian
            @lz4.flush
          end
        end
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
            op = @socket.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
            @acked_bytes.add(len) # byte delta (coalesced), for lag stats
            bump_acked_op(op)     # absolute highest fully-applied op (monotonic)
            # This ack confirms the full_sync baseline is durable, so any records
            # fully covered by the snapshot that mark_op_synced deferred are now
            # durable too — promote the highest into @acked_op.
            if op >= @synced_baseline_op.get && (pending = @pending_synced_op.get) > op
              bump_acked_op(pending)
            end
            unacked_since = nil                # progress; restart the deadline
            @ack_notify.try_send(nil)          # wake any per-follower wait_for_confirm waiter
            @commit_notify.try &.try_send(nil) # wake the server's wait_for_quorum waiters
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
        @ack_notify.close                  # unblock any waiter; this follower is gone
        @flush_requested.close             # stop flush_loop
        @commit_notify.try &.try_send(nil) # re-evaluate the quorum without this follower
        @running.done
      end

      # True once ack_loop has ended (follower disconnected or timed out) or
      # the follower was closed without ack_loop ever running — i.e. it can no
      # longer ack, so it must not be counted as a live replica.
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
        target = @sent_op.get
        request_flush
        until @acked_op.get >= target
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
        Clustering.verify_secret(@socket, password)
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

      # Each streamed record is stamped with its op-number (the leader's global
      # logical-record counter, the same value for every follower) so a follower
      # can ack which op it has durably applied. `op` is passed in by the
      # Server's each_follower dispatch, where it's assigned under @lock.
      def replace(path, op : UInt64) : Int64
        @write_lock.synchronize do
          File.open(File.join(@data_dir, path)) do |file|
            file_size = file.size
            lag_size = (RECORD_OVERHEAD + path.bytesize + file_size).to_i64
            @sent_bytes.add(lag_size)
            @sent_op.set(op)
            send_record_header(op, path)
            @lz4.write_bytes file_size.to_i64, IO::ByteFormat::LittleEndian
            IO.copy(file, @lz4, file_size) == file_size || raise IO::EOFError.new
            lag_size
          end
        end
      end

      def append(path : String, bytes : Bytes, op : UInt64) : Int64
        @write_lock.synchronize do
          lag_size = (RECORD_OVERHEAD + path.bytesize + bytes.bytesize).to_i64
          @sent_bytes.add(lag_size)
          @sent_op.set(op)
          send_record_header(op, path)
          @lz4.write_bytes -bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
          @lz4.write bytes
          lag_size
        end
      end

      def append(path : String, value : UInt32 | Int32, op : UInt64) : Int64
        @write_lock.synchronize do
          lag_size = (RECORD_OVERHEAD + path.bytesize + 4).to_i64
          @sent_bytes.add(lag_size)
          @sent_op.set(op)
          send_record_header(op, path)
          @lz4.write_bytes -4i64, IO::ByteFormat::LittleEndian
          @lz4.write_bytes value, IO::ByteFormat::LittleEndian
          lag_size
        end
      end

      def delete(path, op : UInt64) : Int64
        @write_lock.synchronize do
          lag_size = (RECORD_OVERHEAD + path.bytesize).to_i64
          @sent_bytes.add(lag_size)
          @sent_op.set(op)
          send_record_header(op, path)
          @lz4.write_bytes 0i64 # delete marker (endian-agnostic)
          lag_size
        end
      end

      # A logical record fully covered by this follower's full_sync snapshot is
      # not sent (0 bytes on the wire), but the follower already has it durably,
      # so advance both its sent and acked op so the quorum gate doesn't wait for
      # an ack that will never come. See Server#append's skip handling.
      def mark_op_synced(op : UInt64) : Nil
        @sent_op.set(op)
        # The record's bytes are entirely within the full_sync snapshot, so it's
        # durable on the follower exactly when the snapshot is — i.e. once the
        # follower has acked its baseline. If it has, count the op now; if not,
        # park it as pending so ack_loop promotes it the moment the baseline ack
        # arrives (advancing @acked_op now would count un-persisted bytes toward
        # the commit quorum, risking loss on failover).
        if @acked_op.get >= @synced_baseline_op.get
          bump_acked_op(op)
        else
          bump_pending_synced_op(op)
        end
      end

      # Monotonic max for @acked_op. It has two writers — the ack_loop read fiber
      # and the dispatch fiber (mark_op_synced for fully-skipped records) — and a
      # stale/low op (e.g. a fresh connection's first framing ack before any
      # record completes) must never drive it backwards below the synced baseline.
      private def bump_acked_op(op : UInt64) : Nil
        loop do
          cur = @acked_op.get
          return if op <= cur
          _, ok = @acked_op.compare_and_set(cur, op)
          return if ok
        end
      end

      # Monotonic max for @pending_synced_op (records covered by the snapshot but
      # dispatched before the follower acked its baseline). Same CAS shape as
      # bump_acked_op since the dispatch fiber writes it and ack_loop reads it.
      private def bump_pending_synced_op(op : UInt64) : Nil
        loop do
          cur = @pending_synced_op.get
          return if op <= cur
          _, ok = @pending_synced_op.compare_and_set(cur, op)
          return if ok
        end
      end

      # Wire overhead of a record header: op (UInt64) + path length (Int32) +
      # the payload-length/marker field (Int64).
      RECORD_OVERHEAD = sizeof(UInt64) + sizeof(Int32) + sizeof(Int64)

      private def send_record_header(op : UInt64, path : String)
        @lz4.write_bytes op, IO::ByteFormat::LittleEndian
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
          sent_op:            @sent_op.get,
          acked_op:           @acked_op.get,
          compression_ratio:  @lz4.compression_ratio,
          uncompressed_bytes: @lz4.uncompressed_bytes,
          compressed_bytes:   @lz4.compressed_bytes,
          id:                 @id.to_s(36),
        }.to_json(json)
      end

      def lag_in_bytes : Int64
        @sent_bytes.get - @acked_bytes.get
      end

      # Highest op-number this follower has durably applied and acked.
      def acked_op : UInt64
        @acked_op.get
      end

      # Highest op-number sent to this follower (== the leader's global op for a
      # caught-up follower, since synced followers receive every record).
      def sent_op : UInt64
        @sent_op.get
      end

      def syncing?
        @state.syncing?
      end

      def synced?
        @state.synced?
      end

      # Become Synced as of `baseline_op` — the leader's global op-number at the
      # full_sync cut. We've *sent* the snapshot up to here, but the follower has
      # NOT yet confirmed it is durable: @acked_op must therefore stay where it was
      # (0 for a fresh follower) until the follower acks baseline_op back after
      # syncfs'ing the snapshot. Counting it as acked here would let the leader
      # commit (and confirm) a write on a "quorum" that includes a follower which
      # only received the bytes in flight — if that follower then drops before
      # persisting, the write is durable on fewer than a quorum of nodes and is
      # lost on failover. The follower sends an explicit baseline ack at the start
      # of its stream loop (see Client#stream_changes), which advances @acked_op.
      def mark_synced!(baseline_op : UInt64)
        @sent_op.set(baseline_op)
        @synced_baseline_op.set(baseline_op)
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
