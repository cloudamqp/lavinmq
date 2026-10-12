require "./file_index"
require "./journal"
require "./wire_v3"
require "../config"
require "../rate_limiter"
require "socket"
require "wait_group"

module LavinMQ
  module Clustering
    # The leader's side of a follower connection. After the full sync a
    # sender fiber tails the Journal from the follower's cut and writes the
    # entries to the socket in the negotiated wire format; ack_loop reads the
    # follower's acks: cumulative byte counts for v1/v2, the applied lsn and
    # received bytes for v3 (see WireV3).
    class Follower
      include Journal::Subscriber

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

      # Write timeout used during full_sync. A syncing follower isn't in the ISR
      # yet, so its slowness can't stall publish confirms, and the bulk transfer
      # legitimately blocks the leader's writes while the follower persists the
      # files it receives. The aggressive ACK_TIMEOUT is for the steady-state
      # streaming phase; using it during full_sync wrongly drops a merely-slow
      # follower. Still bounded so a genuinely wedged follower can't hold the
      # sync lock forever.
      SYNC_WRITE_TIMEOUT = 60.seconds

      @acked_bytes = Atomic(Int64).new(0)
      @sent_bytes = Atomic(Int64).new(0)
      # Highest journal entry the follower reports as applied (v3). Seeded
      # with the entry before its join cut: everything older came with the
      # full sync, so a wait for it must not depend on an ack.
      @applied_lsn = Atomic(UInt64).new(0)
      # Wakes the publish-confirm waiter on each ack; closed (never replaced)
      # when ack_loop ends, which doubles as the follower's death signal (see
      # #dead?).
      @ack_notify = ::Channel(Nil).new(1)
      # Wakes the sender; capacity 1 so a burst of appends coalesces into one
      # wakeup. Closed by #close, which ends the sender.
      @wake = ::Channel(Nil).new(1)
      # Next journal lsn the sender reads; entries before it are on the wire
      # (flushed). Written by the sender, read by waiters and the Journal.
      @position = Atomic(UInt64).new(0)
      @write_lock = Mutex.new(:unchecked)
      @running = WaitGroup.new
      @state = State::Syncing
      # Per-file byte offset that this follower already received via full_sync
      # when it was marked synced. Incremental appends below this offset are
      # already in the snapshot and must be skipped to avoid duplicating them.
      @synced_baseline = Hash(String, Int64).new
      # Flush after this many entries even if the journal isn't drained, so
      # a waiter isn't held up behind a long burst
      BATCH_ENTRIES = 1024

      getter id = -1
      getter protocol_version = 1
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
        if @protocol_version < 2
          authenticate_v1!(password)
        else
          authenticate_v2!(password)
        end
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

      # Where a v3 follower says it is: the journal it last streamed and the
      # lsn it has durably applied. The server decides whether to resume.
      def read_resume_request : {UInt64, UInt64}
        WireV3.read_resume_request(@socket)
      end

      def send_resume_reply(resume : Bool, journal_id : UInt64) : Nil
        WireV3.write_resume_reply(@socket, resume, journal_id)
      end

      # Tell a v3 follower, after its full sync, the lsn its stream starts at
      def send_cut(lsn : UInt64) : Nil
        @write_lock.synchronize do
          WireV3.write_cut(@lz4, lsn)
          @lz4.flush
        end
      end

      # `caps` (last sync of a joining follower) limits each file to the byte
      # count recorded as that follower's synced baseline, so the snapshot and
      # the baseline agree and in-flight writes aren't duplicated.
      def full_sync(caps : Hash(String, Int64)? = nil) : Nil
        # Relax the write timeout for the bulk transfer; ack_loop restores the
        # tight ACK_TIMEOUT when the follower enters the streaming phase.
        @socket.write_timeout = SYNC_WRITE_TIMEOUT
        send_file_list(caps: caps)
        send_requested_files(caps: caps)
      end

      def ack_loop(ack_timeout : Time::Span = ACK_TIMEOUT)
        @running.add
        # Tighten the write timeout for the streaming phase; full_sync relaxed it
        # to SYNC_WRITE_TIMEOUT for the bulk transfer.
        @socket.write_timeout = ACK_TIMEOUT
        @socket.read_timeout = 100.milliseconds # Wait for an ack max this time, then check the ack deadline
        # When data is outstanding and unacked, the time we first noticed it.
        # Reset to nil on any ack (progress) or when fully caught up, so the
        # deadline measures time-since-data-became-outstanding, not since the
        # last ack (which would be stale after an idle period).
        unacked_since : Time::Instant? = nil
        # A v3 ack is read piecewise: the read timeout below can split it, and
        # dropping a half-read ack would desync the stream.
        ack = uninitialized UInt8[WireV3::ACK_SIZE]
        filled = 0
        loop do
          if @protocol_version >= 3
            n = @socket.read(ack.to_slice[filled..])
            raise IO::EOFError.new if n.zero?
            filled += n
            next if filled < WireV3::ACK_SIZE
            filled = 0
            lsn, bytes = WireV3.decode_ack(ack.to_slice)
            @applied_lsn.set(lsn) if lsn > @applied_lsn.get
            @acked_bytes.set(bytes) if bytes > @acked_bytes.get
          else
            len = @socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
            @acked_bytes.add(len)
          end
          unacked_since = nil       # progress; restart the deadline
          @ack_notify.try_send(nil) # wake any publish-confirm waiter
        rescue IO::TimeoutError
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
      rescue IO::EOFError | Socket::Error | IO::Error
        # socket closed
      ensure
        @ack_notify.close # unblock any waiter; this follower is gone
        @wake.close       # stop the sender
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

      # Tail the journal from `from` (the lsn the follower was marked synced
      # at) and stream the entries to the follower. Runs in a fiber on the
      # default execution context: the socket's fd belongs to its event loop
      # (ack_loop keeps a read pending on it), so no other context may write
      # it — the Persister's isolated thread only appends to the journal.
      def start_sender(journal : Journal, from : UInt64) : Nil
        self.journal_position = from
        @running.spawn(name: "Clustering follower sender") { send_loop(journal) }
      end

      private def send_loop(journal : Journal) : Nil
        pos = @position.get
        loop do
          batched = 0
          while entry = journal.read(pos)
            begin
              encode(entry)
            ensure
              entry.mfile.try &.release_lease
            end
            pos += 1
            batched += 1
            break if batched >= BATCH_ENTRIES
          end
          if batched.zero? && journal.behind?(pos)
            Log.warn { "Follower id=#{@id.to_s(36)} fell behind the journal, disconnecting" }
            break
          end
          if batched > 0
            @write_lock.synchronize { @lz4.flush }
            @position.set(pos)
            journal.release_unneeded
            @ack_notify.try_send(nil) # waiters re-check what's been sent
            next                      # keep draining before blocking
          end
          @wake.receive
        end
      rescue ::Channel::ClosedError
        # closed: the follower is being dropped
      rescue IO::Error | Socket::Error
        # ack_loop sees the closed socket and marks the follower dead
      ensure
        journal.unsubscribe(self)
        @socket.close rescue nil
      end

      # Journal::Subscriber
      def wake : Nil
        @wake.try_send(nil)
      rescue ::Channel::ClosedError
      end

      def journal_position : UInt64
        @position.get
      end

      def journal_position=(lsn : UInt64)
        @position.set(lsn)
        @applied_lsn.set(lsn - 1) if lsn - 1 > @applied_lsn.get
      end

      def applied_lsn : UInt64
        @applied_lsn.get
      end

      # Push buffered bytes to the follower now rather than at the sender's
      # next drain. Never blocks and never touches the socket, so it's safe
      # to call from any execution context.
      def request_flush : Nil
        wake
      end

      # Block until the follower has acked the journal entry `lsn` (and
      # everything sent before it). Returns true if acked, false if the
      # follower disconnected. A follower that stops acking is disconnected
      # by ack_loop, which closes @ack_notify and unblocks us here.
      def wait_for(lsn : UInt64) : Bool
        request_flush
        # Until the sender has passed lsn, what's been sent doesn't cover it
        until @position.get > lsn
          @ack_notify.receive
        end
        if @protocol_version >= 3
          until @applied_lsn.get >= lsn
            @ack_notify.receive
          end
        else
          # Byte acks: wait for everything sent so far, which covers lsn
          target = @sent_bytes.get
          until @acked_bytes.get >= target
            @ack_notify.receive
          end
        end
        # Several fibers can wait concurrently (the publish confirm loop and
        # definition fences) but each notification wakes only one; pass the
        # wakeup on so every waiter whose target was reached gets to re-check.
        # A waiter still short of its target swallows the relayed wakeup
        # harmlessly — its missing bytes guarantee another ack (or eviction)
        # follows.
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
        case version = Clustering.header_version(slice)
        when 1, 2, 3 then @protocol_version = version
        else
          # Answer with the newest header we speak, so a newer follower can
          # downgrade
          @socket.write(StartV3)
          raise InvalidStartHeaderError.new(slice)
        end
      end

      private def authenticate_v1!(password) : Nil
        len = @socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
        client_password = @socket.read_string(len)
        authenticated!(Crypto::Subtle.constant_time_compare(password, client_password))
      end

      # Versions 2 and 3: echo the follower's header, then a challenge it
      # answers with an HMAC over both, so the secret never crosses the wire
      private def authenticate_v2!(password) : Nil
        header = @protocol_version >= 3 ? StartV3 : StartV2
        challenge = Random::Secure.random_bytes(CHALLENGE_SIZE)
        @socket.write(header + challenge)
        response = Bytes.new(32)
        @socket.read_fully(response)
        authenticated!(Crypto::Subtle.constant_time_compare(response, Clustering.challenge_response(password, challenge, header)))
      end

      private def authenticated!(ok : Bool) : Nil
        if ok
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
        return unless caps
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

      # Write one journal entry in the negotiated wire format. Takes
      # @write_lock so close can't pull the socket from under a record.
      private def encode(entry : Journal::Entry) : Nil
        @write_lock.synchronize do
          if @protocol_version >= 3
            encode_v3(entry)
          else
            encode_v2(entry)
          end
        end
      end

      # The bytes of an append entry. A resuming follower may be sent entries
      # whose mmap lease was released (see Journal#read); those are read from
      # the file, which still holds them unless it was since deleted or
      # rewritten. Then nil: the entry is sent as a delete of the path, which
      # the later delete or replace entry makes right.
      private def append_payload(entry : Journal::Entry) : Bytes?
        return entry.payload if entry.mfile || !entry.op.append?
        bytes = Bytes.new(entry.len)
        File.open(File.join(@data_dir, entry.path)) do |f|
          f.read_at(entry.offset, entry.len) do |io|
            io.read_fully(bytes)
          end
        end
        bytes
      rescue File::NotFoundError | IO::EOFError
        Log.debug { "#{entry.path} no longer holds the bytes of journal entry #{entry.lsn}, sending it as a delete" }
        nil
      end

      # Every record carries its lsn and an append its offset, so the follower
      # applies it idempotently: nothing is skipped against the join cut, the
      # bytes it already has are simply rewritten with the same content.
      private def encode_v3(entry : Journal::Entry) : Nil
        size = case entry.op
               in .append?, .append_bytes?
                 if bytes = append_payload(entry)
                   WireV3.write_append(@lz4, entry.lsn, entry.path, entry.offset, bytes)
                 else
                   WireV3.write_delete(@lz4, entry.lsn, entry.path)
                 end
               in .append_value?
                 buf = uninitialized UInt8[4]
                 IO::ByteFormat::LittleEndian.encode(entry.value, buf.to_slice)
                 WireV3.write_append(@lz4, entry.lsn, entry.path, entry.offset, buf.to_slice)
               in .replace?
                 WireV3.write_replace(@lz4, entry.lsn, entry.path, entry.bytes)
               in .delete?
                 WireV3.write_delete(@lz4, entry.lsn, entry.path)
               in .fsync?
                 WireV3.write_fsync(@lz4, entry.lsn, entry.path)
               end
        @sent_bytes.add(size)
      end

      # Appends carry no offset in v1/v2, so the ones below this follower's
      # join cut are skipped or tail-sliced (see #already_synced).
      private def encode_v2(entry : Journal::Entry) : Nil
        case entry.op
        in .append?, .append_bytes?
          if bytes = append_payload(entry)
            skip = already_synced(entry.path, entry.offset, bytes.bytesize.to_i64)
            write_append(entry.path, bytes[skip..]) if skip < bytes.bytesize
          else
            write_delete(entry.path)
            forget_baseline(entry.path)
          end
        in .append_value?
          case skip = already_synced(entry.path, entry.offset, entry.len)
          when 0         then write_append(entry.path, entry.value)
          when entry.len then nil # entirely within the follower's full_sync snapshot
          else
            # A 4-byte value is written in a single call, so a cut inside it
            # shouldn't happen; stay byte-exact anyway and send the tail.
            buf = uninitialized UInt8[4]
            IO::ByteFormat::LittleEndian.encode(entry.value, buf.to_slice)
            write_append(entry.path, buf.to_slice[skip..])
          end
        in .replace?
          write_replace(entry.path, entry.bytes)
          # The whole file was just resent; a synced baseline for it
          # (captured at this follower's join) no longer describes its
          # content and would wrongly skip appends at offsets below the old
          # cut (e.g. after a definitions compaction shrinks the file).
          forget_baseline(entry.path)
        in .delete?
          write_delete(entry.path)
          forget_baseline(entry.path) # path may be reused by a future file
        in .fsync?
          # Version 1 followers sync before every ack and don't understand
          # fsync requests
          write_fsync(entry.path) if @protocol_version >= 2
        end
      end

      # Replace a file's whole content. A positive length on the wire marks a
      # replace; the follower streams it to <path>.tmp then renames atomically.
      # An empty slice writes a 0 length, which the follower applies as a delete
      # (an emptied file carries no data to restore anyway).
      private def write_replace(path : String, bytes : Bytes) : Nil
        @sent_bytes.add(sizeof(Int32) + path.bytesize + sizeof(Int64) + bytes.bytesize)
        send_filename(path)
        @lz4.write_bytes bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
        @lz4.write bytes
      end

      private def write_append(path : String, bytes : Bytes) : Nil
        @sent_bytes.add(sizeof(Int32) + path.bytesize + sizeof(Int64) + bytes.bytesize)
        send_filename(path)
        @lz4.write_bytes -bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
        @lz4.write bytes
      end

      private def write_append(path : String, value : UInt32) : Nil
        @sent_bytes.add(sizeof(Int32) + path.bytesize + sizeof(Int64) + 4)
        send_filename(path)
        @lz4.write_bytes -4i64, IO::ByteFormat::LittleEndian
        @lz4.write_bytes value, IO::ByteFormat::LittleEndian
      end

      private def write_delete(path : String) : Nil
        @sent_bytes.add(sizeof(Int32) + path.bytesize + sizeof(Int64))
        send_filename(path)
        @lz4.write_bytes 0i64 # delete marker (endian-agnostic)
      end

      # Ask the follower to fsync `path` (relative to the data dir, "" for
      # the whole data dir) before it acks past this point in the stream.
      private def write_fsync(path : String) : Nil
        @sent_bytes.add(sizeof(Int32) + 1 + path.bytesize + sizeof(Int64))
        @lz4.write_bytes (path.bytesize + 1).to_i32, IO::ByteFormat::LittleEndian
        @lz4.write_byte FSYNC_PREFIX.ord.to_u8
        @lz4.write path.to_slice
        @lz4.write_bytes 0i64
      end

      private def send_filename(path)
        @lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
        @lz4.write path.to_slice
      end

      def close
        begin
          # Don't @lz4.close here: it writes the end-of-frame and flushes to
          # the socket, which blocks (and re-raises the write timeout) when the
          # follower stopped reading and the send buffer is full — the exact
          # case that drops a follower mid full_sync. That used to skip the
          # @socket.close below, leaving the socket open so the follower never
          # saw a disconnect and never reconnected. close always means the
          # follower is being dropped and will reconnect + re-sync, so a clean
          # frame-end has no value and any buffered (hence unacked, non-durable)
          # data is safely discarded. Just close the socket; the LZ4 writer's
          # context is freed by its finalizer.
          @write_lock.synchronize do
            @socket.close
          end
        rescue IO::Error
          # ignore connection errors while closing
        end
        @wake.close # stop the sender; a sender blocked on a write is released by the socket close above
        @running.wait
        # Normally ack_loop's ensure has closed these by now, but a follower
        # whose ack_loop never ran (e.g. its join failed after mark_synced!)
        # must still read as dead and unblock any wait_for waiter, or a
        # publish confirm could hang on it forever.
        @ack_notify.close
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
          protocol_version:   @protocol_version,
          applied_lsn:        @applied_lsn.get,
        }.to_json(json)
      end

      def lag_in_bytes : Int64
        @sent_bytes.get - @acked_bytes.get
      end

      def sent_bytes : Int64
        @sent_bytes.get
      end

      def acked_bytes : Int64
        @acked_bytes.get
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
