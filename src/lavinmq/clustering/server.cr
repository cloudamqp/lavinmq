require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
require "./checksums"
require "./coordinator"
require "./journal"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
require "lz4"
require "sync/shared"

module LavinMQ
  module Clustering
    # A connecting follower sends a start header and authenticates (see
    # Follower#negotiate!), then syncs: the server lists its files with their
    # SHA1 hashes and the follower requests the ones it lacks or that differ.
    # Writes go on meanwhile, into the Journal. A second, capped pass sends
    # what changed during the first, and the follower's sender then streams
    # the journal (appends, replaced and deleted files, fsync requests) from
    # the cut taken for that pass, which the follower acks.
    class Server
      include FileIndex
      include Replicator
      Log = LavinMQ::Log.for "clustering.server"

      # Guards @followers and @dirty_isr
      @lock = Mutex.new(:unchecked)
      @fenced = Atomic(Bool).new(false)
      @broker_closing = Atomic(Bool).new(false)
      @sync_lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      getter journal = Journal.new
      @password : String
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

      # Registered with the coordinator while this server is open
      @member_removed : Int32 ->

      def initialize(config : Config, @coordinator : Coordinator, @id : Int32)
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @password = password
        @file_index = Sync::Shared.new({Hash(String, MFile?).new, Checksums.new(@data_dir)}, :unchecked)
        @member_removed = ->drop_follower(Int32)
        @coordinator.add_member_removed_listener(@member_removed)
      end

      # Disconnects a follower that was removed from the cluster. It stays out
      # of the ISR and is refused when it reconnects.
      private def drop_follower(id : Int32) : Nil
        follower = @lock.synchronize { @followers.find { |f| f.id == id } }
        return unless follower
        Log.warn { "Disconnecting follower id=#{id.to_s(36)}, it was removed from the cluster" }
        follower.close
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

      def register_file(mfile : MFile) : String
        path = strip_datadir mfile.path
        mfile.replication_path = path
        @file_index.lock do |files, checksums|
          files[path] = mfile
          checksums.delete(path)
        end
        path
      end

      # Whether the file is in the replicated set, i.e. would reach a follower
      def registered?(path : String) : Bool
        path = strip_datadir path
        @file_index.shared { |files, _checksums| files.has_key?(path) }
      end

      # Regular files, re-read from disk. The content is copied now, so a
      # later rewrite of the same file can't change what this one sends. A
      # file that's gone is journaled as a delete.
      def replace_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
        if bytes = read_file(File.join(@data_dir, path))
          @journal.replace(path, bytes)
        else
          @journal.delete(path)
        end
        dispatched
      end

      # Replace an mmap-backed file's entire content on all followers — used
      # after an in-place compaction (e.g. a stream's consumer_offsets file).
      # Unlike replace_file(path) the MFile stays registered, so the
      # zero-syscall append(path, pos, length) overload keeps working for later
      # writes, and the bytes are copied from the mmap capped at mfile.size
      # (File.size would be the sparse ftruncate capacity, not the data length).
      def replace_file(mfile : MFile)
        path = strip_datadir mfile.path
        mfile.replication_path = path
        bytes = mfile.to_slice.dup
        @file_index.lock do |files, checksums|
          files[path] = mfile
          checksums.delete(path)
        end
        @journal.replace(path, bytes)
        dispatched
      end

      private def read_file(path : String) : Bytes?
        File.open(path, &.getb_to_end)
      rescue File::NotFoundError
        nil
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
        @journal.append_mfile(path, mfile, pos.to_i64, length.to_i64)
        dispatched
      end

      # Replicate `length` bytes written at `pos` in a registered MFile. No
      # allocation: the relative path is cached on the file.
      def append(mfile : MFile, pos : Int, length : Int)
        path = mfile.replication_path || register_file(mfile)
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        @journal.append_mfile(path, mfile, pos.to_i64, length.to_i64)
        dispatched
      end

      def append_value(mfile : MFile, value : UInt32 | Int32, offset : Int64)
        path = mfile.replication_path || register_file(mfile)
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        @journal.append_value(path, value.to_u32!, offset)
        dispatched
      end

      # Replicate a small in-memory integer (e.g. Schema::VERSION, ack
      # position). The follower writes it directly to its LZ4 stream — no
      # heap allocation. `offset` is where the value is written on the leader,
      # used to skip bytes a just-joined follower already got via full_sync.
      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        @journal.append_value(path, value.to_u32!, offset)
        dispatched
      end

      # Replicate an in-memory byte buffer. The buffer is copied; see
      # append_value for `offset`.
      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        @journal.append_bytes(path, bytes.dup, offset)
        dispatched
      end

      def delete_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files.delete(path)
          checksums.delete(path)
        end
        @journal.delete(path)
        dispatched
      end

      def delete_dir(path : String)
        prefix = "#{strip_datadir path}/"
        deleted = @file_index.lock do |files, checksums|
          paths = files.keys.select!(&.starts_with?(prefix))
          paths.each do |p|
            files.delete(p)
            checksums.delete(p)
          end
          paths
        end
        deleted.each { |p| @journal.delete(p) }
        dispatched
      end

      def nr_of_files
        @file_index.shared { |files, _checksums| files.size }
      end

      # With `caps` (the last full_sync of a joining follower) each file is
      # hashed over its first `caps[path]` bytes only, the cut the follower
      # is marked synced at, so a write journaled after the cut is streamed
      # instead of duplicated. Files absent from `caps` are capped at 0.
      def files_with_hash(caps : Hash(String, Int64)? = nil, & : Tuple(String, Bytes) -> Nil)
        # Snapshot the index to allow safe iteration without holding the lock.
        # Files are read from disk via File handles (no MFile mmap reads), so
        # concurrent close of the MFile is harmless. For sparse MFile-backed
        # files, we cap the hash at mfile.size to match the follower's
        # contiguous (non-sparse) copy.
        snapshot = @file_index.shared { |files, _checksums| files.dup }
        sha1 = Digest::SHA1.new
        snapshot.each do |path, mfile|
          cap = caps ? caps[path]? || 0i64 : nil
          if cached_hash = cached_hash?(path, cap)
            yield({path, cached_hash})
          else
            filename = File.join(@data_dir, path)
            begin
              hashed_size = 0i64
              File.open(filename) do |f|
                size = mfile ? mfile.size : f.size.to_i64
                size = Math.min(size, cap) if cap
                hashed_size = size.to_i64
                sha1.update IO::Sized.new(f, size)
              end
              hash = sha1.final
              sha1.reset
              @file_index.lock { |_files, checksums| checksums.set(path, hash, hashed_size) } unless caps
              yield({path, hash})
            rescue File::NotFoundError
              next # File disappeared since we took the snapshot, just skip it.
            end
          end
        end
      end

      # Reuse a cached hash only when its recorded coverage fits: exactly
      # `cap` bytes for a capped pass, any coverage otherwise. Checking
      # a cap against the file's current size instead would race local writes
      # that haven't invalidated the cache yet.
      private def cached_hash?(path : String, cap : Int64?) : Bytes?
        @file_index.shared do |_files, checksums|
          if entry = checksums[path]?
            entry.hash if cap.nil? || entry.size == cap
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

      # The current logical size of every indexed file (mfile.size for sparse
      # MFiles, else File.size), taken right after a joining follower's
      # journal cut, so it covers every entry before the cut: a write lands
      # in its file before it is journaled. A size landing inside a record
      # not yet journaled is fine, Follower#already_synced streams the tail.
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

      # The position this node had as a follower, meaningless once it leads
      POSITION_FILE = ".replication_position"

      def listen(server : TCPServer)
        server.listen
        # called before accepting followers, no lock needed
        @file_index.lock { |_files, checksums| checksums.restore }
        File.delete? File.join(@data_dir, POSITION_FILE)
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
        follower.negotiate!(@password)
        if follower.id == @id
          Log.error { "Disconnecting follower with the clustering id of the leader" }
          return
        end
        unless @coordinator.member?(follower.id)
          Log.warn { "Refusing follower id=#{follower.id.to_s(36)}, it is not a member of the cluster" }
          return
        end
        resume_from = negotiate_resume(follower)
        @lock.synchronize do
          if stale_follower = @followers.find { |f| f.id == follower.id }
            Log.error { "Disconnecting stale follower with id #{follower.id.to_s(36)}" }
            @followers.delete(stale_follower)
            stale_follower.close
          end
          @followers << follower # Starts in Syncing state
        end
        if resume_from
          resume_and_serve(follower, resume_from)
        else
          sync_and_serve(follower)
        end
      rescue AuthenticationError
        Log.warn { "Follower negotiation error" }
      rescue ex : InvalidStartHeaderError
        Log.warn { ex.message }
      rescue IO::EOFError
        Log.info { "Follower disconnected" }
      rescue ex : IO::Error
        Log.warn(exception: ex) { "Follower disonnected: #{ex.message}" }
      rescue Coordinator::StaleLeadership
        Log.info { "Not the leader anymore, disconnecting the follower" }
      ensure
        follower.try &.close
      end

      # A v3 follower reports the journal and lsn it has applied up to. When
      # that's this journal and every later entry is still in it, the stream
      # resumes from there and the full sync is skipped. Returns the lsn to
      # resume from, nil for a full sync.
      private def negotiate_resume(follower : Follower) : UInt64?
        return if follower.protocol_version < 3
        journal_id, applied = follower.read_resume_request
        resume = journal_id == @journal.id && @journal.resumable?(applied)
        follower.send_resume_reply(resume, @journal.id)
        if resume
          Log.info { "Follower id=#{follower.id.to_s(36)} resumes from journal entry #{applied + 1}" }
          applied + 1
        else
          Log.info { "Follower id=#{follower.id.to_s(36)} needs a full sync" } if journal_id != 0
          nil
        end
      end

      # Serve a follower that has everything up to `from`: stream from there.
      private def resume_and_serve(follower : Follower, from : UInt64) : Nil
        @journal.subscribe(follower, from)
        @lock.synchronize do
          follower.mark_synced!
          update_isr
        end
        follower.start_sender(@journal)
        follower.ack_loop
      ensure
        cleanup(follower)
      end

      # Full-sync an already-registered follower into the Synced state, then
      # serve its ack loop until it disconnects or is closed. Writers are
      # never blocked by a join: the second pass is capped at a journal cut,
      # and everything journaled after the cut reaches the follower through
      # its sender.
      private def sync_and_serve(follower : Follower) : Nil
        # Only allow one follower to do full sync at a time
        # The bandwidth between nodes should be very high, so
        # better with one fully synced than 2 partially synced followers
        # @sync_lock is always acquired before @lock to avoid deadlock
        @sync_lock.synchronize do
          follower.full_sync # sync the bulk
          # From here on the journal keeps the entries for this follower
          @journal.subscribe(follower)
          cut_lsn = @journal.head
          # The per-file cut is taken after the journal cut and caps the last
          # sync, so what full_sync sends is exactly the baseline and covers
          # every entry before cut_lsn, see #snapshot_sizes.
          sizes = snapshot_sizes
          follower.full_sync(sizes) # sync the last, capped at the cut
          follower.capture_synced_baseline(sizes)
          follower.send_cut(cut_lsn) if follower.protocol_version >= 3
          @lock.synchronize do
            follower.mark_synced! # Change state to Synced
            update_isr
          end
          follower.start_sender(@journal)
        end
        # Wait for follower to disconnect or be closed
        follower.ack_loop
      ensure
        cleanup(follower)
      end

      private def cleanup(follower : Follower) : Nil
        @journal.unsubscribe(follower)
        # Covers everything after registration, including a full_sync that
        # raised or an update_isr that failed right after mark_synced! — a
        # follower left in @followers as Synced with no ack_loop running
        # would hang every wait_for forever.
        @lock.synchronize do
          @followers.delete(follower)
          if follower.synced?
            # If the follower was behind (unacked replicated data) when it
            # dropped, it may be missing data that's about to be confirmed via
            # the surviving followers, so it must leave the ISR now rather
            # than lazily — otherwise it could be promoted on failover lacking
            # already-confirmed data. A caught-up follower (no lag) still has
            # everything confirmed so far, so we leave it in the ISR as a valid
            # failover candidate; the dirty ISR is flushed before the next
            # replicated durable operation returns (dispatched) and before
            # the next publish confirm (Persister), so nothing it lacks is
            # ever acknowledged while it remains listed.
            behind = follower.lag_in_bytes > 0 || follower.journal_position < @journal.head
            @dirty_isr = true
            if behind
              begin
                update_isr # @dirty_isr stays set, so the lazy path retries on failure
              rescue Coordinator::StaleLeadership
                Log.debug { "Not the leader anymore, ISR not updated after follower id=#{follower.id.to_s(36)} disconnected" }
              rescue ex
                Log.warn(exception: ex) { "Failed to update ISR after follower id=#{follower.id.to_s(36)} disconnected" }
              end
            end
          end
        end
      end

      private def update_isr
        ids = Set(Int32).new
        @followers.each do |f|
          # A dead follower may linger in @followers until its handler fiber
          # runs its cleanup; it must not re-enter the ISR meanwhile (flush_isr
          # races that cleanup when a confirm is pending).
          ids.add(f.id) if f.synced? && !f.dead? && @coordinator.member?(f.id)
        end
        ids.add(@id)
        # Closed, its followers were disconnected: they mustn't leave the ISR
        # for that, e.g. the target of a leadership handover
        raise_if_fenced
        Log.info { "In-sync replicas: #{ids.to_a}" }
        begin
          @coordinator.update_isr(ids)
        rescue ex : Coordinator::StaleLeadership
          @fenced.set(true)
          raise ex
        end
        @dirty_isr = false
      end

      private def raise_if_fenced : Nil
        raise Coordinator::StaleLeadership.new("Replication is fenced, nothing can be acknowledged") if fenced?
      end

      # True when the ISR last written to the coordinator may be stale (a
      # follower connected or disconnected since). Checked by the Persister
      # before sending publish confirms.
      def isr_dirty? : Bool
        @lock.synchronize { @dirty_isr }
      end

      # Commit the current ISR to the coordinator, retrying until it succeeds
      # or this node is no longer the leader (Coordinator::StaleLeadership is
      # raised then, the operation can't be acknowledged anymore). Called
      # before any durable operation is acknowledged once a synced follower
      # has disconnected (by #dispatched and #wait_for_followers): the
      # acknowledgment may only go out once the follower's removal is
      # durable, or a leader crash right after it could elect that follower
      # without the acknowledged data. Operations stall rather than being
      # acknowledged against a stale ISR; if the coordinator stays
      # unreachable the leader's lease expires and the process exits.
      def flush_isr : Nil
        loop do
          @lock.synchronize { update_isr }
          return
        rescue ex : Coordinator::StaleLeadership
          raise ex
        rescue ex
          Log.warn(exception: ex) { "Failed to update ISR, retrying" }
          sleep 0.5.seconds
        end
      end

      # Ask the followers to fsync these paths before acking past this point
      # in the stream. Journaled, so the request is ordered after the writes
      # it covers; only the journal mutex is taken, never a socket, so the
      # Persister's isolated thread can call it.
      def request_fsync(paths : Enumerable(String)) : Nil
        paths.each { |p| @journal.fsync(strip_datadir p) }
      end

      def request_syncfs : Nil
        @journal.fsync("")
      end

      def fenced? : Bool
        @fenced.get
      end

      def broker_closing : Nil
        @broker_closing.set(true)
      end

      # Block until every in-sync follower has acked everything journaled so
      # far, so a durable operation may be acknowledged to a client: every
      # node the ISR lists as a failover candidate then has it on disk. A
      # follower that disconnected meanwhile, or earlier leaving the ISR
      # dirty, may lack it, so its removal is committed first (#flush_isr).
      # Raises Coordinator::StaleLeadership once this node isn't the leader
      # anymore, so the operation fails rather than being acknowledged.
      def wait_for_followers : Nil
        raise_if_fenced
        lsn = @journal.last_lsn
        all_acked = true
        followers.each { |f| all_acked &= f.wait_for(lsn) }
        flush_isr if !all_acked || isr_dirty?
        # Closed while waiting: what the followers didn't ack isn't replicated
        raise_if_fenced
      end

      def wait_for_acks(timeout : Time::Span) : Bool
        return false if fenced?
        acked = Channel(Nil).new(1)
        waiting = followers
        lsn = @journal.last_lsn
        spawn(name: "Wait for follower acks") do
          waiting.each &.wait_for(lsn) # until acked or disconnected
          acked.send nil
        end
        select
        when acked.receive
          true
        when timeout(timeout)
          false
        end
      end

      def close
        @fenced.set(true)
        # A node makes a new server for each term it leads, the coordinator
        # outlives them
        @coordinator.remove_member_removed_listener(@member_removed)
        @listeners.each &.close
        @lock.synchronize do
          @followers.each &.close
          @followers.clear
        end
        Fiber.yield # required for follower/listener fibers to actually finish
        @file_index.lock { |_files, checksums| checksums.store }
      end

      # Called after a change is journaled: commits a dirty ISR (see
      # #flush_isr) before the change can be acknowledged, so no follower
      # that disconnected before it was journaled is still listed. Raises
      # Coordinator::StaleLeadership once this node isn't the leader
      # anymore, unless the broker is closing, see #broker_closing.
      private def dispatched : Nil
        # Nothing it replicates can be acknowledged, and the ISR may not be
        # ours to change anymore, e.g. the broker closing after leadership
        # was lost
        return if fenced?
        return unless isr_dirty?
        begin
          flush_isr
        rescue ex : Coordinator::StaleLeadership
          # Fenced now, so nothing more is replicated or acknowledged
          raise ex unless @broker_closing.get
          Log.info { "Not the leader anymore, the rest of the broker's close isn't replicated" }
        end
      end

      private def strip_datadir(path : String) : String
        path[@data_dir.bytesize + 1..]
      end
    end
  end
end
