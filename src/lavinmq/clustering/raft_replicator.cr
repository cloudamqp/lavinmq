require "./replicator"
require "./follower"
require "./raft_coordinator"
require "./raft/node"
require "./raft/ops"
require "../config"
require "../filesystem"

module LavinMQ
  module Clustering
    # Replicates the leader's data dir through the Raft log: the broker's
    # changes to its files become op batches (Raft::Ops) in entries, their
    # bytes stay in the files and travel as the entries' payload. A
    # replicator per term this node leads, see RaftController#new_replicator.
    #
    # The broker's calls only add a few bytes to the open batch, under a
    # lock, and wake the proposer fiber, which seals the batch and proposes
    # it. Batches grow while a proposal is in flight. A LeaderSyncer fsyncs
    # the files the batches touched and tells the node, which only counts
    # itself towards committing entries whose payload is on its disk.
    # #wait_for_followers waits for everything so far to be committed, a
    # majority having it on disk.
    #
    # What the broker deletes is moved to the trash instead, see
    # Raft::FileApplier, so it can be put back if the entry is never
    # committed. A replaced file's old content is kept the same way, see
    # #preserve_before_replace.
    class RaftReplicator
      include Replicator
      Log = LavinMQ::Log.for "clustering.replicator"

      # Payload and ops of one entry
      ENTRY_PAYLOAD_MAX = 1i64 << 20
      ENTRY_OPS_MAX     = 256 << 10
      # Appends from a buffer up to this size go inline, larger ones are
      # read from the file
      INLINE_MAX = 64 << 10

      private record Sealed, data : Bytes, paths : Set(String)

      @lock = Mutex.new(:unchecked)
      @batch = Raft::Ops::Batch.new
      # Files the open batch touched, for the syncer
      @dirty = Set(String).new
      @sealed = Deque(Sealed).new
      # Batches sealed and proposed, and the index of the last proposed
      @sealed_count = 0i64
      @proposed_count = 0i64
      @proposed_index = 0i64
      # Waiting for a batch to be proposed: its number and where to tell the
      # index, nil if it never will be
      @waiters = Array(Tuple(Int64, Channel(Int64?))).new
      @signal = Channel(Bool).new(1)
      @files = Hash(String, Raft::FileTable::FileEntry).new
      @next_id : UInt32
      @tid_seq = 0u32
      # Paths whose old content #preserve_before_replace put in the trash
      @preserved = Hash(String, Int64).new
      @scratch = Bytes.new(4)
      @fenced = Atomic(Bool).new(false)
      @broker_closing = Atomic(Bool).new(false)
      @syncer : LeaderSyncer
      @data_dir : String
      @term : Int64

      def initialize(@node : Raft::Node, @password : String, data_dir : String)
        @data_dir = data_dir.rstrip('/')
        @term = @node.term
        table = @node.files || raise RaftCoordinator::StaleLeadership.new
        table.each { |e| @files[e.path] = e unless e.staging? }
        @next_id = table.next_id
        @syncer = LeaderSyncer.new(@node, @data_dir)
        spawn(name: "Raft replicator proposer") { propose_loop }
        spawn(name: "Raft replicator fence") { watch_serving }
      end

      def password : String
        @password
      end

      def fenced? : Bool
        @fenced.get
      end

      def broker_closing : Nil
        @broker_closing.set(true)
      end

      # The followers aren't connected to the replicator, see Raft::Node
      def followers : Array(Follower)
        Array(Follower).new
      end

      def syncing_followers : Array(Follower)
        Array(Follower).new
      end

      def all_followers : Array(Follower)
        Array(Follower).new
      end

      def isr_dirty? : Bool
        false
      end

      def flush_isr : Nil
      end

      # Every ack is durable, nothing to request
      def request_fsync(paths : Enumerable(String)) : Nil
      end

      def request_syncfs : Nil
      end

      def confirms_by_commit? : Bool
        true
      end

      def register_file(path : String)
        register(path, nil)
      end

      def register_file(file : File)
        register(file.path, nil)
      end

      def register_file(mfile : MFile)
        register(mfile.path, mfile.size.to_i64)
      end

      def replace_file(path : String)
        replace(path, File.size(path).to_i64)
      end

      def replace_file(mfile : MFile)
        replace(mfile.path, mfile.size.to_i64)
      end

      def append(path : String, pos : Int, length : Int)
        record_op(path) do |rel|
          e = @files[rel]? || next register_locked(rel, path, nil)
          append_range(e, pos.to_i64, length.to_i64)
        end
      end

      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        record_op(path) do |rel|
          e = @files[rel]? || next register_locked(rel, path, nil)
          IO::ByteFormat::LittleEndian.encode(value, @scratch)
          append_inline(e, offset, @scratch)
        end
      end

      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        record_op(path) do |rel|
          e = @files[rel]? || next register_locked(rel, path, nil)
          if bytes.size <= INLINE_MAX
            append_inline(e, offset, bytes)
          else
            append_range(e, offset, bytes.size.to_i64)
          end
        end
      end

      # Moves the file to the trash, see the class description
      def delete_file(path : String)
        record_op(path) do |rel|
          e = @files.delete(rel) || next
          tid = next_tid
          move_to_trash(path, rel, tid)
          @batch.delete(e.id, tid)
        end
      end

      def delete_dir(path : String)
        record_op(path) do |rel|
          prefix = "#{rel}/"
          @files.reject! { |p, _| p == rel || p.starts_with?(prefix) }
          tid = next_tid
          move_to_trash(path, rel, tid)
          @batch.delete_dir(tid, rel)
        end
      end

      # Keeps what's at *path* in the trash before it's replaced, so that
      # it's still there for a follower that hasn't got it yet, or for putting
      # back if the replacement is never committed. Called before the rename.
      def preserve_before_replace(path : String) : Nil
        return if fenced?
        @lock.synchronize do
          rel = relative(path)
          return unless @files.has_key?(rel) && File.exists?(path)
          tid = next_tid
          trashed = trash_path(tid, rel)
          Dir.mkdir_p(File.dirname(trashed))
          File.link(path, trashed)
          @preserved[rel] = tid
        end
      rescue ex : File::Error
        Log.warn { "Could not keep #{path} before replacing it: #{ex.message}" }
      end

      # Waits for everything replicated so far to be committed, see the class
      # description. Raises when this node isn't the leader anymore.
      def wait_for_followers : Nil
        index = proposed_index_now || raise RaftCoordinator::StaleLeadership.new
        return if index.zero?
        unless @node.await_commit(index, @term)
          @fenced.set(true)
          raise RaftCoordinator::StaleLeadership.new
        end
      end

      def wait_for_acks(timeout : Time::Span) : Bool
        return false if fenced?
        done = Channel(Bool).new(1)
        spawn(name: "Wait for commit") do
          wait_for_followers
          done.send true
        rescue
          done.send false
        end
        select
        when result = done.receive
          result
        when timeout(timeout)
          false
        end
      end

      # Snapshots are served from here, see the snapshot server
      def listen(server : TCPServer)
        @listener = server
        while socket = server.accept?
          socket.close
        end
      end

      @listener : TCPServer? = nil

      def clear
      end

      def close
        @fenced.set(true)
        @listener.try &.close
        @signal.close
        @syncer.close
        @lock.synchronize do
          @waiters.each(&.[1].send(nil))
          @waiters.clear
        end
      end

      # Runs *block* with the path relative to the data dir under the lock,
      # unless fenced, then has the batch proposed
      private def record_op(path : String, & : String ->) : Nil
        return if fenced?
        @lock.synchronize do
          # In the batches it seals too
          @dirty << path
          yield relative(path)
          @dirty << path
        end
        signal
      end

      private def register(path : String, size : Int64?) : Nil
        record_op(path) { |rel| register_locked(rel, path, size) unless @files.has_key?(rel) }
      end

      # A file the log doesn't know yet, with what's in it
      private def register_locked(rel : String, path : String, size : Int64?) : Nil
        size ||= File.info?(path).try(&.size.to_i64) || 0i64
        e = new_entry(rel)
        @batch.create(e.id, rel)
        append_range(e, 0i64, size) if size > 0
      end

      # A staged file, installed in the same entry: the leader has the bytes
      # at *path* only, so a follower must never get the staging part alone.
      # That entry's payload can be larger than ENTRY_PAYLOAD_MAX then.
      private def replace(path : String, size : Int64) : Nil
        record_op(path) do |rel|
          seal
          @files.delete(rel)
          tid = @preserved.delete(rel) || next_tid
          e = new_entry(rel)
          @batch.create(e.id, "", Raft::Ops::STAGING)
          @batch.append(e.id, 0i64, size) if size > 0
          e.size = size
          @batch.install(e.id, tid, rel)
          seal
        end
      end

      private def new_entry(rel : String) : Raft::FileTable::FileEntry
        id = @next_id
        @next_id += 1
        e = Raft::FileTable::FileEntry.new(id, rel)
        @files[rel] = e
        e
      end

      # Appends a range, in ops no larger than an entry's payload, sealing
      # the batch when it's full
      private def append_range(e : Raft::FileTable::FileEntry, pos : Int64, len : Int64) : Nil
        while len > 0
          room = ENTRY_PAYLOAD_MAX - @batch.payload_bytes
          if room <= 0
            seal
            room = ENTRY_PAYLOAD_MAX
          end
          chunk = Math.min(len, room)
          @batch.append(e.id, pos, chunk)
          pos += chunk
          len -= chunk
        end
        e.size = Math.max(e.size, pos)
        seal if @batch.bytesize >= ENTRY_OPS_MAX
      end

      private def append_inline(e : Raft::FileTable::FileEntry, pos : Int64, bytes : Bytes) : Nil
        @batch.append_inline(e.id, pos, bytes)
        e.size = Math.max(e.size, pos + bytes.size)
        seal if @batch.bytesize >= ENTRY_OPS_MAX
      end

      # Moves the open batch to the ones to propose. Under the lock.
      private def seal : Nil
        return if @batch.empty?
        @sealed << Sealed.new(@batch.to_slice.dup, @dirty)
        @dirty = Set(String).new
        @batch.clear
        @sealed_count += 1
      end

      private def signal : Nil
        select
        when @signal.send(true)
        else
        end
      rescue Channel::ClosedError
      end

      # The index of the entry with everything replicated until now, once
      # it's proposed, 0 when nothing ever was. Nil when it won't be.
      private def proposed_index_now : Int64?
        return if fenced?
        ch = Channel(Int64?).new(1)
        @lock.synchronize do
          seal
          return @proposed_index if @sealed_count == @proposed_count
          @waiters << {@sealed_count, ch}
        end
        signal
        ch.receive
      end

      private def propose_loop : Nil
        while @signal.receive?
          loop do
            batch = @lock.synchronize do
              seal if @sealed.empty?
              @sealed.shift?
            end
            break unless batch
            proposed = @node.propose_data(batch.data)
            unless proposed && proposed[1] == @term
              fence
              return
            end
            @syncer.written(proposed[0], batch.paths)
            @lock.synchronize do
              @proposed_count += 1
              @proposed_index = proposed[0]
              @waiters.reject! do |w|
                next false if w[0] > @proposed_count
                w[1].send @proposed_index
                true
              end
            end
          end
        end
      end

      private def watch_serving : Nil
        @node.serving.when_false.receive?
        fence
      end

      private def fence : Nil
        return if @fenced.swap(true)
        Log.info { "Not the leader anymore, nothing more is replicated" }
        @lock.synchronize do
          @waiters.each(&.[1].send(nil))
          @waiters.clear
        end
      end

      private def next_tid : Int64
        @tid_seq += 1
        (@term << 32) | @tid_seq
      end

      private def relative(path : String) : String
        return path unless path.starts_with?(@data_dir)
        path[@data_dir.bytesize..].lchop('/')
      end

      private def trash_path(tid : Int64, rel : String) : String
        File.join(@data_dir, Raft::FileApplier::TRASH_DIR, tid.to_s, rel)
      end

      private def move_to_trash(path : String, rel : String, tid : Int64) : Nil
        return unless File.exists?(path)
        trashed = trash_path(tid, rel)
        Dir.mkdir_p(File.dirname(trashed))
        File.rename(path, trashed)
        @dirty << File.dirname(path) << File.dirname(trashed)
      rescue ex : File::Error
        Log.warn { "Could not move #{path} to the trash: #{ex.message}" }
      end
    end

    # Fsyncs the files the leader's proposed batches touched, and tells the
    # node which entries' payload is on disk, see Raft::Node#local_durable.
    # On a thread of its own, like the Persister, as fsyncs block.
    class LeaderSyncer
      private record Written, index : Int64, paths : Set(String)

      @queue = Channel(Written).new(1024)

      def initialize(@node : Raft::Node, @data_dir : String)
        @context = Fiber::ExecutionContext::Isolated.new("Raft leader sync") { run }
      end

      def written(index : Int64, paths : Set(String)) : Nil
        @queue.send Written.new(index, paths)
      rescue Channel::ClosedError
      end

      def close : Nil
        @queue.close
      end

      private def run : Nil
        while first = @queue.receive?
          index = first.index
          paths = first.paths
          loop do
            select
            when more = @queue.receive?
              break unless more
              index = more.index
              paths.concat more.paths
            else
              break
            end
          end
          sync(paths) if Config.instance.sync?
          @node.local_durable(index)
        end
      end

      private def sync(paths : Set(String)) : Nil
        if paths.size > Config.instance.syncfs_threshold
          File.open(@data_dir) { |dir| FileSystem.syncfs(dir.fd) }
        else
          paths.each do |path|
            File.open(path, &.fsync)
          rescue File::NotFoundError
            # moved to the trash meanwhile, its directory is synced
          end
        end
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
    end
  end
end
