require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
require "./checksums"
require "./coordinator"
require "./vr/membership"
require "./vr/node"
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

      @lock = Mutex.new(:unchecked)
      @sync_lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      @password : String
      # Global op-number: a monotonic counter of logical replication records (the
      # VR op-number / viewstamp position). Incremented once per dispatched record
      # in each_follower, under @lock, and stamped on the record sent to every
      # follower so acks are globally comparable. Guarded by @lock.
      @op = 0u64
      @id : Int32
      @config : Config
      # Maps relative paths to their MFile (for sparse, mmap-backed files) or
      # nil (for regular files where File.size is authoritative). MFile-backed
      # segments are sparse: ftruncate'd to capacity then appended from the
      # beginning, so we need mfile.size to know the real data length and
      # mfile.to_slice to read from the mmap. Full-sync paths always read from
      # disk via fresh File handles; only the append hot path reads the mmap.
      @file_index : Sync::Shared(Tuple(Hash(String, MFile?), Checksums))

      # The VR consensus node, set by the Controller after construction. Used
      # only to surface clustering status over the HTTP API (leader discovery).
      property vr_node : VR::Node? = nil

      # Persistent VR state, set by the Controller. The leader seeds its op
      # counter from it (so op-numbers continue across leader changes rather than
      # restarting at 0) and persists the committed op as it advances, so a
      # crashed/restarted node reports its true durable position in elections.
      property vr_state : VR::State? = nil
      # The committed op last persisted to vr_state, so we only fsync on advance.
      @persisted_commit_op = 0u64

      # {node_id, role, view, op, commit_op, primary_id, primary_uri} or nil when
      # this isn't a VR cluster.
      def clustering_status
        @vr_node.try(&.status)
      end

      # Quorum size (majority of the configured roster) for majority-quorum
      # commit. Nil while the legacy etcd path drives durability; set once the VR
      # coordinator wires in the membership. When set, wait_for_quorum gates
      # durable operations on a majority having the data rather than on all
      # in-sync followers.
      @quorum : Int32?
      # Highest op-number a quorum has durably applied. Guarded by @lock.
      @commit_op = 0u64
      # Coalescing wakeup for wait_for_quorum waiters: any follower ack (or a
      # follower disconnect, which may change the quorum) fires it. Capacity 1 so
      # a burst collapses to one wakeup; relayed between waiters like
      # Follower#@ack_notify.
      @commit_notify = ::Channel(Nil).new(1)

      def initialize(config : Config, @coordinator : Coordinator, @id : Int32, @quorum : Int32? = nil)
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @password = password
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

      def replace_file(path : String) # only non mfiles are ever replaced
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
        each_follower do |f, op|
          f.replace(path, op)
          # The whole file was just resent; a synced baseline for it (captured
          # at this follower's join) no longer describes its content and would
          # wrongly skip appends at offsets below the old cut (e.g. after a
          # definitions compaction shrinks the file).
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
        each_follower do |f, op|
          skip = f.already_synced(path, offset, bytes.size.to_i64)
          if skip < bytes.size
            f.append(path, bytes[skip..], op)
          else
            f.mark_op_synced(op) # entirely within the follower's full_sync snapshot
          end
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
        each_follower do |f, op|
          case skip = f.already_synced(path, offset, size)
          when 0    then f.append(path, value, op)
          when size then f.mark_op_synced(op) # entirely within the follower's full_sync snapshot
          else
            # A 4-byte value is written in a single call, so a cut inside it
            # shouldn't happen; stay byte-exact anyway and send the tail.
            buf = uninitialized UInt8[4]
            IO::ByteFormat::LittleEndian.encode(value, buf.to_slice)
            f.append(path, buf.to_slice[skip..], op)
          end
        end
      end

      # Replicate an in-memory byte buffer. Caller owns the buffer. See
      # append_value for `offset`.
      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower do |f, op|
          skip = f.already_synced(path, offset, bytes.bytesize.to_i64)
          if skip < bytes.bytesize
            f.append(path, bytes[skip..], op)
          else
            f.mark_op_synced(op) # entirely within the follower's full_sync snapshot
          end
        end
      end

      def delete_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files.delete(path)
          checksums.delete(path)
        end
        each_follower do |f, op|
          f.delete(path, op)
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

      def password : String
        @coordinator.password
      end

      @listeners = Array(TCPServer).new(1)

      def listen(server : TCPServer)
        server.listen
        restore_checksums
        Log.info { "Listening on #{server.local_address}" }
        @listeners << server

        loop do
          socket = server.accept? || break
          spawn(name: "Clustering follower") { handle_socket(socket) }
        end
      end

      # Load the on-disk checksum cache before accepting followers. Called by
      # #listen, and by the Controller before this node starts serving followers
      # when it owns the shared clustering listener. Idempotent enough — no lock
      # needed since it runs before any follower connects.
      def restore_checksums : Nil
        @file_index.lock { |_files, checksums| checksums.restore }
        # Continue op-numbering from this node's persisted high-water so op-numbers
        # are globally monotonic across leader changes (they must never restart at
        # 0, or a fresh leader would look "behind" a synced follower in elections
        # and a follower's ack of a low op could be misread). The data this op
        # refers to is reconciled byte-exact by full_sync, so exact alignment
        # isn't required — only that op never goes backward.
        if state = @vr_state
          @lock.synchronize do
            seeded = state.op
            @op = seeded if seeded > @op
            @commit_op = state.commit_op if state.commit_op > @commit_op
            @persisted_commit_op = @commit_op
          end
        end
      end

      private def handle_socket(socket : TCPSocket)
        validate_start_header(socket) || return
        accept_follower(socket)
      end

      # Serve a follower whose REPLI start header has already been read and
      # validated (by Server#listen above, or by the shared clustering listener
      # that routes REPLI vs VRCTL connections). Does the auth + id exchange,
      # registers the follower, and serves its ack loop.
      def accept_follower(socket : TCPSocket) : Nil
        Log.context.set(follower: socket.remote_address.to_s)
        follower = Follower.new(socket, @data_dir, self, @commit_notify)
        follower.negotiate_after_header!(@password)
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
      rescue ex : AuthenticationError
        Log.warn { "Follower negotiation error" }
      rescue ex : IO::EOFError
        Log.info { "Follower disconnected" }
      rescue ex : IO::Error
        Log.warn(exception: ex) { "Follower disonnected: #{ex.message}" }
      ensure
        follower.try &.close
      end

      # Read and check the 8-byte REPLI start header. Returns false (and closes)
      # on mismatch.
      private def validate_start_header(socket : TCPSocket) : Bool
        header = uninitialized UInt8[8]
        socket.read_fully(header.to_slice)
        return true if header.to_slice == Start
        Log.warn { "Invalid start header from #{socket.remote_address}" }
        socket.close
        false
      rescue IO::Error
        false
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
            # The op-number at the cut: the follower has every record up to here
            # via the snapshot, so it becomes synced fully-acked at this op. Read
            # under @lock, so it's stable from the cut through mark_synced!.
            baseline_op = @op
            # Send the capped snapshot AND the baseline op, so the follower adopts
            # this op as its high-water (it now holds everything up to here).
            follower.full_sync(cut, baseline_op)
            follower.capture_synced_baseline(cut)
            follower.mark_synced!(baseline_op) # Change state to Synced
          end
        end
        # A newly synced follower may complete a pending quorum; re-evaluate.
        @commit_notify.try_send(nil)
        # Wait for follower to disconnect or be closed
        follower.ack_loop
      ensure
        # Remove the follower; a disconnect may shrink the live set below quorum
        # (handled by wait_for_quorum, which stalls). committed_op recomputes
        # from the remaining followers — the dropped one simply stops counting.
        @lock.synchronize { @followers.delete(follower) }
        @commit_notify.try_send(nil)
      end

      # Block until a quorum has durably applied everything replicated so far,
      # so a durable operation may be acknowledged to a client. With a roster
      # quorum configured (the VR cluster) this is majority-quorum commit; with
      # none (a stand-in/legacy Server in specs) it waits for all live followers.
      def wait_for_followers : Nil
        if @quorum
          wait_for_quorum(current_op)
        else
          followers.each &.wait_for_confirm
        end
      end

      # Majority-quorum durability gate (the VR commit rule), used once a roster
      # quorum is configured. Blocks until a quorum (this leader + enough synced
      # followers) has durably applied everything up to `target_op`, then
      # returns. Already-committed targets return at once. Reuses the standing
      # ack fibers via @commit_notify — no per-operation fiber, no polling. A
      # minority that cannot form a quorum blocks here (CP: the operation stalls
      # rather than being acknowledged on a minority) until members (re)sync or
      # the server closes.
      def wait_for_quorum(target_op : UInt64) : Nil
        until committed_op >= target_op
          @commit_notify.receive
        end
        @commit_notify.try_send(nil) # relay to other waiters (see Follower#wait_for_confirm)
      rescue ::Channel::ClosedError
        # server closing; stop waiting
      end

      # The current commit point: the highest op-number a quorum has durably
      # applied. The leader counts itself at @op (it has everything it assigned);
      # each synced follower contributes its acked op. With no roster configured
      # (legacy etcd path) there is nothing to gate, so it reports @op. Updates
      # the cached @commit_op (monotonic) for heartbeat gossip.
      def committed_op : UInt64
        q = @quorum
        return @op unless q
        advanced = false
        commit = @lock.synchronize do
          positions = Array(UInt64).new(@followers.size + 1)
          positions << @op
          @followers.each { |f| positions << f.acked_op if f.synced? }
          if c = VR::Membership.committed_op(q, positions)
            if c > @commit_op
              @commit_op = c
              advanced = true
            end
          end
          @commit_op
        end
        # Persist the committed point (durably, outside @lock) when it advances so
        # a crash/restart of this leader reports its true durable position in the
        # next election — never higher than committed (committed data is on a
        # quorum, hence locally durable), so it can't claim data it lacks.
        if advanced && (state = @vr_state) && commit > @persisted_commit_op
          @persisted_commit_op = commit
          state.save(op: commit, commit_op: commit)
        end
        commit
      end

      # A non-blocking read of the last-known commit point, for the VR heartbeat
      # and election path. Unlike #committed_op it takes neither @lock nor an
      # fsync, so the VR::Node may call it while holding its own FSM lock without
      # risking a stall — #committed_op can block for the duration of a follower's
      # full_sync (which holds @lock) or an fsync, and blocking the FSM there
      # freezes heartbeats and triggers a spurious election storm. The value may
      # be momentarily stale, which is safe: commit_op is a monotonic lower bound
      # used only for gossip and election ordering, while the authoritative
      # advance + durable persist still happens in #committed_op on the confirm
      # path (wait_for_quorum).
      def committed_op_cached : UInt64
        @commit_op
      end

      # The latest op-number assigned by this leader (its own log head).
      def current_op : UInt64
        @lock.synchronize { @op }
      end

      def close
        @listeners.each &.close
        @lock.synchronize do
          @followers.each &.close
          @followers.clear
        end
        Fiber.yield          # required for follower/listener fibers to actually finish
        @commit_notify.close # unblock any wait_for_quorum waiters
        begin
          @file_index.lock { |_files, checksums| checksums.store }
        rescue ::Channel::ClosedError
          # close() can run during process teardown after the async Log backend's
          # dispatch channel has already been closed; store logs, so that surfaces
          # as a ClosedError. The checksum file is only a perf cache (recomputed on
          # next start), so dropping its final store here is harmless — and far
          # better than an "Unhandled exception" that aborts an orderly shutdown.
        end
      end

      # Dispatch a replicated change to all synced followers, assigning the
      # record its global op-number. Durability is gated separately, when the
      # operation is acknowledged, by wait_for_followers (majority-quorum
      # commit) — not here.
      private def each_follower(& : Follower, UInt64 -> Nil) : Nil
        @lock.synchronize do
          op = @op &+= 1 # one op-number per logical record, assigned under @lock
          @followers.each do |f|
            next if f.syncing? # Performing a full sync
            yield f, op
          rescue IO::Error | Socket::Error
            Log.info { "Follower disconnected address=#{f.remote_address} id=#{f.id.to_s(36)}" }
            Fiber.yield # Allow other fiber to run to remove the follower from array
          end
        end
      end

      private def strip_datadir(path : String) : String
        path[@data_dir.bytesize + 1..]
      end
    end
  end
end
