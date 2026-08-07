require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
require "./checksums"
require "./coordinator"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
require "lz4"
require "sync/shared"

module LavinMQ
  module Clustering
    # When a follower connects:
    # It sends a static header (wrong header disconnects the client)
    # It sends its password (servers closes the connection if the password is wrong)
    # Server sends a list of files in its data directory and the sha1 hash of those files
    # Client requests files that is missing or has mismatching checksums of
    # In the meantime the server queues up changes (all publishes/consumes are paused)
    # When client doesn't request more files starts to stream changes
    # Server sends "appends", which include the file path and the bytes to be appended
    # It also sends which files should be deleted or has been rewritten (such as json files)
    # The follower sends back/acknowledges how many bytes it has received
    class Server
      include FileIndex
      include Replicator
      Log = LavinMQ::Log.for "clustering.server"

      # Raised mid-join when the ISR commit fails after mark_synced!, to abort
      # the join and let handle_socket's ensure drop the follower.
      class ISRCommitError < Exception; end

      @lock = Mutex.new(:unchecked)
      @sync_lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      @dirty_isr = true
      @id : Int32
      @config : Config
      # Maps relative paths to their MFile (for sparse, mmap-backed files) or
      # nil (for regular files where File.size is authoritative). MFile-backed
      # segments are sparse: ftruncate'd to capacity then appended from the
      # beginning, so we need mfile.size to know the real data length and
      # mfile.to_slice to read from the mmap. Full-sync paths always read from
      # disk via fresh File handles; only the append hot path reads the mmap.
      @file_index : Sync::Shared(Tuple(Hash(String, MFile?), Checksums))

      # Lazily fetched from @coordinator on first access. The raft coordinator
      # can't answer at construction time (the raft node isn't leader yet —
      # bootstrap happens later, inside the elector). By the time a follower
      # connects, raft has elected and the leader has written the shared secret
      # to its .clustering_password file.
      getter password : String { @coordinator.password }

      def initialize(config : Config, @coordinator : Coordinator, @id : Int32)
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @file_index = Sync::Shared.new({Hash(String, MFile?).new, Checksums.new(@data_dir)}, :unchecked)
      end

      def clear
        @file_index.lock do |files, checksums|
          files.clear
          checksums.clear
        end
      end

      def register_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
      end

      def register_file(file : File)
        register_file file.path
      end

      def register_file(mfile : MFile)
        path = strip_datadir mfile.path
        @file_index.lock do |files, checksums|
          files[path] = mfile
          checksums.delete(path)
        end
      end

      def replace_file(path : String) # regular files, re-read from disk
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
        each_follower do |f|
          f.replace(path)
          # The whole file was just resent; a synced baseline for it (captured
          # at this follower's join) no longer describes its content and would
          # wrongly skip appends at offsets below the old cut (e.g. after a
          # definitions compaction shrinks the file).
          f.forget_baseline(path)
        end
      end

      # Replace an mmap-backed file's entire content on all followers — used
      # after an in-place compaction (e.g. a stream's consumer_offsets file).
      # Unlike replace_file(path) the MFile stays registered, so the
      # zero-syscall append(path, pos, length) overload keeps working for later
      # writes, and the bytes are read from the mmap capped at mfile.size
      # (File.size would be the sparse ftruncate capacity, not the data length).
      def replace_file(mfile : MFile)
        path = strip_datadir mfile.path
        bytes = mfile.to_slice
        @file_index.lock do |files, checksums|
          files[path] = mfile
          checksums.delete(path)
        end
        each_follower do |f|
          f.replace(path, bytes)
          f.forget_baseline(path)
        end
      end

      # Replicate `length` bytes from `path` starting at `pos`. The bytes are
      # read from the registered MFile's mmap — zero syscalls. Callers must
      # have registered the path as an MFile first; for regular Files use the
      # `append(path, bytes)` overload instead.
      def append(path : String, pos : Int, length : Int)
        path = strip_datadir path
        mfile = @file_index.lock do |files, checksums|
          checksums.delete(path)
          files[path]?
        end
        raise ArgumentError.new("append(pos, length) requires an MFile-registered path: #{path}") unless mfile
        bytes = mfile.to_slice(pos.to_i64, length.to_i64)
        offset = pos.to_i64
        each_follower do |f|
          skip = f.already_synced(path, offset, bytes.size.to_i64)
          f.append(path, bytes[skip..]) if skip < bytes.size
        end
      end

      # Replicate a small in-memory integer (e.g. Schema::VERSION, ack
      # position). The follower writes it directly to its LZ4 stream — no
      # heap allocation. `offset` is where the value is written on the leader,
      # used to skip bytes a just-joined follower already got via full_sync.
      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        size = sizeof(Int32).to_i64 # value is 4 bytes on the wire (Int32 or UInt32)
        each_follower do |f|
          case skip = f.already_synced(path, offset, size)
          when 0    then f.append(path, value)
          when size then next # entirely within the follower's full_sync snapshot
          else
            # A 4-byte value is written in a single call, so a cut inside it
            # shouldn't happen; stay byte-exact anyway and send the tail.
            buf = uninitialized UInt8[4]
            IO::ByteFormat::LittleEndian.encode(value, buf.to_slice)
            f.append(path, buf.to_slice[skip..])
          end
        end
      end

      # Replicate an in-memory byte buffer. Caller owns the buffer. See
      # append_value for `offset`.
      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower do |f|
          skip = f.already_synced(path, offset, bytes.bytesize.to_i64)
          f.append(path, bytes[skip..]) if skip < bytes.bytesize
        end
      end

      def delete_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files.delete(path)
          checksums.delete(path)
        end
        each_follower do |f|
          f.delete(path)
          f.forget_baseline(path) # path may be reused by a future file
        end
      end

      def nr_of_files
        @file_index.shared { |files, _checksums| files.size }
      end

      # When `caps` is given (the last full_sync of a joining follower), each
      # file's hash is computed over only the first `caps[path]` bytes — the
      # cut the follower will be marked synced at. This excludes writes whose
      # replication dispatch hasn't happened yet (local write at
      # message_store.cr:339 races full_sync because it doesn't hold @lock), so
      # they are delivered incrementally instead of duplicated. Files absent
      # from `caps` are capped at 0 (delivered entirely via the stream).
      def files_with_hash(caps : Hash(String, Int64)? = nil, & : Tuple(String, Bytes) -> Nil)
        # Snapshot the index to allow safe iteration without holding the lock.
        # Files are read from disk via File handles (no MFile mmap reads), so
        # concurrent close of the MFile is harmless. For sparse MFile-backed
        # files, we cap the hash at mfile.size to match the follower's
        # contiguous (non-sparse) copy.
        snapshot = @file_index.shared { |files, _checksums| files.dup }
        sha1 = Digest::SHA1.new
        snapshot.each do |path, mfile|
          # The cache holds full-size hashes; a capped pass must recompute.
          cached_hash = caps ? nil : @file_index.shared { |_files, checksums| checksums[path]? }
          if cached_hash
            yield({path, cached_hash})
          else
            filename = File.join(@data_dir, path)
            begin
              File.open(filename) do |f|
                size = mfile ? mfile.size : f.size.to_i64
                size = Math.min(size, caps[path]? || 0i64) if caps
                sha1.update IO::Sized.new(f, size)
              end
              hash = sha1.final
              sha1.reset
              @file_index.lock { |_files, checksums| checksums[path] = hash } unless caps
              yield({path, hash})
            rescue File::NotFoundError
              next # File disappeared since we took the snapshot, just skip it.
            end
          end
        end
      end

      # Yields the file (or nil if missing) along with its real data size in
      # bytes. For MFile-backed sparse files the size comes from mfile.size,
      # not from File.size which would be the capacity. `cap` (see
      # files_with_hash) limits the yielded size for a capped full_sync.
      def with_file(filename, cap : Int64? = nil, & : File?, Int64 -> _)
        # has_key? needed because a registered path with a nil MFile is
        # different from a path that's not in the index at all.
        has_key, mfile = @file_index.shared { |files, _checksums| {files.has_key?(filename), files[filename]?} }
        if has_key
          path = File.join(@data_dir, filename)
          begin
            File.open(path) do |f|
              f.read_buffering = false
              size = mfile ? mfile.size : f.size.to_i64
              size = Math.min(size, cap) if cap
              yield f, size
            end
          rescue File::NotFoundError
            yield nil, 0i64
          end
        else
          yield nil, 0i64
        end
      end

      # Snapshot the current logical size of every indexed file, using the same
      # size rule as full_sync (mfile.size for sparse MFiles, else File.size).
      # Captured at mark_synced to record the per-file cut a joining follower
      # received via full_sync. A live size may land *inside* a record that a
      # writer has written locally but not yet dispatched — that's fine: the
      # snapshot delivers the head bytes and Follower#already_synced makes the
      # later append stream only the tail. Callers hold @lock.
      private def snapshot_sizes : Hash(String, Int64)
        sizes = Hash(String, Int64).new
        @file_index.shared do |files, _checksums|
          files.each do |path, mfile|
            if mfile
              sizes[path] = mfile.size.to_i64
            else
              begin
                sizes[path] = File.size(File.join(@data_dir, path)).to_i64
              rescue File::NotFoundError
                next
              end
            end
          end
        end
        sizes
      end

      def followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.synced?) # select returns new array => thread safe
        end
      end

      def syncing_followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.syncing?) # select returns new array => thread safe
        end
      end

      def all_followers : Array(Follower)
        @lock.synchronize do
          @followers.dup # for thread safety
        end
      end

      @listeners = Array(TCPServer).new(1)

      def listen(server : TCPServer)
        server.listen
        # called before accepting followers, no lock needed
        @file_index.lock { |_files, checksums| checksums.restore }
        Log.info { "Listening on #{server.local_address}" }
        @listeners << server

        loop do
          socket = server.accept? || break
          spawn(name: "Clustering follower") { handle_socket(socket) }
        end
      end

      private def handle_socket(socket : TCPSocket)
        Log.context.set(follower: socket.remote_address.to_s)
        follower = Follower.new(socket, @data_dir, self)
        follower.negotiate!(password)
        if follower.id == @id
          Log.error { "Disconnecting follower with the clustering id of the leader" }
          return
        end
        @lock.synchronize do
          if stale_follower = @followers.find { |f| f.id == follower.id }
            Log.error { "Disconnecting stale follower with id #{follower.id.to_s(36)}" }
            @followers.delete(stale_follower)
            stale_follower.close
          end
          @followers << follower # Starts in Syncing state
        end
        sync_and_serve(follower)
      rescue ISRCommitError
        # ISR commit failed mid-join; follower is dropped by the ensure below
        # and re-syncs on reconnect. Not an error condition for the leader.
        Log.info { "Aborted follower join: ISR commit failed (will retry on reconnect)" }
      rescue ex : AuthenticationError
        Log.warn { "Follower negotiation error" }
      rescue ex : InvalidStartHeaderError
        Log.warn { ex.message }
      rescue ex : IO::EOFError
        Log.info { "Follower disconnected" }
      rescue ex : IO::Error
        Log.warn(exception: ex) { "Follower disonnected: #{ex.message}" }
      ensure
        follower.try &.close
      end

      # Full-sync an already-registered follower into the Synced state, then
      # serve its ack loop until it disconnects or is closed.
      private def sync_and_serve(follower : Follower) : Nil
        # Only allow one follower to do full sync at a time
        # The bandwidth between nodes should be very high, so
        # better with one fully synced than 2 partially synced followers
        # @sync_lock is always acquired before @lock to avoid deadlock
        @sync_lock.synchronize do
          follower.full_sync # sync the bulk
          @lock.synchronize do
            # Capture the per-file cut BEFORE the last sync and cap the sync to
            # it, so what full_sync sends equals the baseline exactly. A larger
            # mfile.size from an in-flight write (local write done, replication
            # dispatch still pending) is excluded here and delivered via the
            # stream instead of being duplicated — wholly, or just the record's
            # tail if the cut landed mid-record (Follower#already_synced).
            cut = snapshot_sizes
            follower.full_sync(cut) # sync the last, capped at the cut
            follower.capture_synced_baseline(cut)
            follower.mark_synced! # Change state to Synced
            # A failed ISR commit right after mark_synced! must abort the join:
            # the `ensure` below then drops the follower rather than leaving it
            # Synced and listed while the coordinator never recorded it. A typed
            # exception so handle_socket logs it cleanly instead of letting a
            # bare Exception escape the fiber as an unhandled backtrace.
            raise ISRCommitError.new unless update_isr
          end
        end
        # Wait for follower to disconnect or be closed
        follower.ack_loop
      ensure
        # Covers everything after registration, including a full_sync that
        # raised or an update_isr that failed right after mark_synced! — a
        # follower left in @followers as Synced with no ack_loop running
        # would hang every wait_for_confirm forever.
        @lock.synchronize do
          @followers.delete(follower)
          if follower.synced?
            # If the follower was behind (unacked replicated data) when it
            # dropped, it may be missing data that's about to be confirmed via
            # the surviving followers, so it must leave the etcd ISR now rather
            # than lazily — otherwise it could be promoted on failover lacking
            # already-confirmed data. A caught-up follower (no lag) still has
            # everything confirmed so far, so we leave it in the ISR as a valid
            # failover candidate; the dirty ISR is flushed before the next
            # replicated durable operation returns (each_follower) and before
            # the next publish confirm (Persister), so nothing it lacks is
            # ever acknowledged while it remains listed.
            behind = follower.lag_in_bytes > 0
            @dirty_isr = true
            # Returns false on failure; @dirty_isr stays set so the lazy path
            # (each_follower / Persister) retries before the next ack.
            update_isr if behind
          end
        end
      end

      private def update_isr : Bool
        ids = Set(Int32).new
        @followers.each do |f|
          # A dead follower may linger in @followers until its handler fiber
          # runs its cleanup; it must not re-enter the ISR meanwhile (flush_isr
          # races that cleanup when a confirm is pending).
          ids.add(f.id) if f.synced? && !f.dead?
        end
        ids.add(@id)
        Log.info { "In-sync replicas: #{ids.to_a}" }
        committed = @coordinator.update_isr(ids)
        @dirty_isr = false if committed
        committed
      end

      # True when the ISR last written to the coordinator may be stale (a
      # follower connected or disconnected since). Checked by the Persister
      # before sending publish confirms.
      def isr_dirty? : Bool
        @lock.synchronize { @dirty_isr }
      end

      # Commit the current ISR to the coordinator, retrying until it succeeds.
      # Called before any durable operation is acknowledged when a synced
      # follower has disconnected — by each_follower after dispatching a
      # replicated change, and by the Persister before sending publish
      # confirms: the acknowledgment may only go out once the follower's
      # removal from the ISR is durable, otherwise a leader crash right after
      # the acknowledgment could elect that follower even though it lacks the
      # acknowledged data. Operations must stall rather than be acknowledged
      # against a stale ISR — if the coordinator stays unreachable the
      # leader's lease eventually expires and the process exits.
      def flush_isr : Nil
        until @lock.synchronize { update_isr }
          Log.warn { "Failed to update ISR, retrying" }
          sleep 0.5.seconds
        end
      end

      # Block until every in-sync follower has acked everything replicated so
      # far, so a durable operation may be acknowledged to a client: once
      # this returns, every node etcd lists as a failover candidate has the
      # operation durably on disk. Wait for all followers (no short-circuit)
      # — wait_for_confirm blocks until the follower acks or disconnects. A
      # follower that disconnected (wait_for_confirm == false, or it dropped
      # earlier and left the ISR dirty) may lack data that's about to be
      # acknowledged, so its removal must be committed to the coordinator
      # before this returns (see flush_isr).
      def wait_for_followers : Nil
        all_acked = true
        followers.each { |f| all_acked &= f.wait_for_confirm }
        flush_isr if !all_acked || isr_dirty?
      end

      def close
        @listeners.each &.close
        @lock.synchronize do
          @followers.each &.close
          @followers.clear
        end
        Fiber.yield # required for follower/listener fibers to actually finish
        @file_index.lock { |_files, checksums| checksums.store }
      end

      # Dispatch a replicated change to all synced followers, then commit a
      # dirty ISR before returning. Every durable operation replicates its
      # change before acknowledging it (definitions writes, JSON file
      # replaces, segment deletes, publishes), so flushing here guarantees no
      # operation is acknowledged while etcd still lists a follower that
      # disconnected before this change was dispatched — a leader crash right
      # after the acknowledgment could otherwise elect that follower without
      # the acknowledged change. The etcd write happens after the dispatch
      # loop, so a coordinator failure can't abort a dispatch halfway and
      # leave a hole in every follower's file, and flush_isr retries instead
      # of raising into the publish path — the operation stalls, and if the
      # coordinator stays unreachable the leader's lease expires and the
      # process exits.
      private def each_follower(& : Follower -> Nil) : Nil
        dirty = false
        @lock.synchronize do
          broken = nil
          @followers.each do |f|
            next if f.syncing? # Performing a full sync
            yield f
          rescue IO::Error | Socket::Error
            Log.info { "Follower disconnected address=#{f.remote_address} id=#{f.id.to_s(36)}" }
            # Remove the dead follower inline: the lock is already held and
            # @lock guards every @followers mutation, so a Fiber.yield here
            # can't hand off to a removal path. The follower's handler fiber
            # ensure remains the safety net (its @followers.delete becomes a
            # no-op). A synced follower leaving dirties the ISR so it's
            # committed before this operation is acknowledged.
            (broken ||= Array(Follower).new) << f
            @dirty_isr = true if f.synced?
          end
          broken.try &.each { |f| @followers.delete(f) }
          dirty = @dirty_isr
        end
        flush_isr if dirty
      end

      private def strip_datadir(path : String) : String
        path[@data_dir.bytesize + 1..]
      end
    end
  end
end
