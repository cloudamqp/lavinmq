require "./config"
require "./logger"
require "./mfile"
require "./filesystem"
require "./clustering/replicator"
require "./clustering/follower"
require "sync/exclusive"
require "../stdlib/isolated_wait_for"

module LavinMQ
  # Makes confirmed publishes durable before they are confirmed. A single
  # Persister is created per Server and shared between all VHosts.
  #
  # Only the files a confirm depends on are synced (msync of the segments
  # written by confirmed publishes, plus the directories of newly created
  # files), not the whole filesystem: a syncfs would also flush every ack
  # file that consumers append to, writing the same pages again and again.
  # When many files are dirty, or a transaction is committed (which must also
  # persist its acks), one syncfs is cheaper and simpler than many msyncs.
  class Persister
    Log = LavinMQ::Log.for "persister"

    # Receives publish confirms once the confirmed data is durable
    module ConfirmTarget
      abstract def enqueue_confirm_ack(msgid : UInt64) : Nil
    end

    private class Batch
      # Confirm acks, by target, accumulated since the last drain. The
      # follower set is decided at drain time against the in-sync set as it
      # exists then (see Clustering::Server#wait_for_followers), which is safe
      # because a follower only reaches the in-sync set after a full_sync that
      # includes every prior write.
      getter acks = Hash(ConfirmTarget, UInt64).new
      getter files = Array(MFile).new
      getter paths = Set(String).new
      getter? syncfs = false
      # Callers of #sync, released by the drain that synced for them
      getter waiters = Array(::Channel(Nil)).new
      # A store waits for this drain, see #copy_drain
      property? requested = false

      def drainable? : Bool
        requested? || !acks.empty? || !waiters.empty? || !files.empty? || !paths.empty?
      end

      # Past the syncfs threshold a drain syncs the whole filesystem and
      # doesn't look at the paths, so they stop being collected there, which
      # bounds them on a server that rarely confirms
      def add_path(path : String) : Nil
        if paths.size < Config.instance.syncfs_threshold || paths.includes?(path)
          paths.add(path)
        else
          @syncfs = true
        end
      end
    end

    {% unless flag?(:release) %}
      record SyncRecord, syncfs : Bool, paths : Array(String)
      # Spec instrumentation: what the most recent drain that synced synced
      getter last_sync : SyncRecord?
      # Spec instrumentation: calls of #sync_local
      getter local_syncs = 0
      # Spec instrumentation: every drain that synced, oldest first
      @syncs = Array(SyncRecord).new
      @syncs_lock = Mutex.new

      def syncs : Array(SyncRecord)
        @syncs_lock.synchronize { @syncs.dup }
      end
    {% end %}

    # Drains that took a batch, counted under the @pending lock, and the
    # latest of them that synced it, see #copy_drain
    @drains_started = 0u64
    @drains_synced = Atomic(UInt64).new(0u64)
    # A drain skipped its sync while sync was disabled, and the files it took
    # are no longer marked, so the next drain that syncs does a syncfs
    @skipped_sync = false
    # Callers of #wait_drain, with the drain they wait for
    @drain_waiters = Sync::Exclusive(Array({UInt64, ::Channel(Nil)})).new(Array({UInt64, ::Channel(Nil)}).new, :unchecked)

    @data_dir_fd : Int32 = -1
    @publish_confirm_requested = ::Channel(Bool).new(1)
    # Acks, dirty files and sync waiters share one lock, so a drain swaps out
    # every file marked before the acks it confirms
    @pending = Sync::Exclusive(Batch).new(Batch.new, :unchecked)

    def initialize(@data_dir : String, @replicator : Clustering::Replicator? = nil)
      @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
      raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      # Run on a dedicated thread so the blocking syscalls only stall this
      # thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
    end

    # Every confirm — sync, no-sync, and clustered alike — is routed through the
    # publish confirm loop so each target has exactly one producer of ack
    # frames. `sync` is a runtime-mutable INI option (SIGHUP reloads it); taking
    # a shortcut here when it is disabled would let a later direct ack overtake
    # an earlier batched one after a mid-stream flip, sending cumulative
    # Basic.Ack frames out of delivery-tag order (see #2078). The loop skips the
    # actual sync while sync is disabled (see drain), so no-sync only pays a
    # single hop to the loop, not a disk flush.
    def enqueue_ack(target : ConfirmTarget, id : UInt64)
      @pending.lock { |batch| batch.acks[target] = id }
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
    end

    # Register a file whose written data a later confirm depends on. Must be
    # called after the write is dispatched to the replicator, so the fsync
    # request that the drain sends to followers is behind it in the stream.
    def mark_dirty(mfile : MFile) : Nil
      return if mfile.mark_needs_msync!
      @pending.lock &.files.push(mfile)
    end

    # Like mark_dirty(MFile), for regular (non-mmapped) files and for
    # directories with new entries. Marks are deduplicated until the next
    # drain, so many new entries in one directory cost one fsync.
    def mark_dirty(path : String) : Nil
      @pending.lock &.add_path(path)
    end

    # Makes *mfile* and *path* durable in a drain of their own, without
    # waiting for a confirm to request one. A stream calls it when a segment
    # is finished: confirms only sync the segments they wrote to, so an
    # unconfirmed segment could otherwise stay unsynced while later ones
    # become durable, and a power loss would leave a hole in the offsets.
    # The directory is synced too, so the entry of the segment opened after
    # it is durable with the meta file.
    def sync_soon(mfile : MFile, path : String) : Nil
      return unless Config.instance.sync?
      mark_dirty(mfile)
      mark_dirty(path)
      mark_dirty(File.dirname(path))
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
    end

    # For a message the broker copied to another queue (dead-lettered,
    # delayed, retried), after the copy was written and marked dirty there.
    # Returns the number of the drain that makes the copy durable, see
    # #drain_synced?. It doesn't request that drain: a store does when it
    # has to keep a segment for it (see #request_drain), so a burst of moves
    # costs a drain per emptied segment, not one per message.
    def copy_drain : UInt64
      @pending.lock { @drains_started + 1 }
    end

    # Whether the drain numbered *seq* by #copy_drain, or a later one, has
    # synced, here and on the in-sync followers
    def drain_synced?(seq : UInt64) : Bool
      @drains_synced.get(:acquire) >= seq
    end

    # Requests a drain even if nothing else is pending: one that syncs is
    # needed for a copy whose drain ran while sync was disabled. Returns
    # false once the loop has exited at shutdown.
    def request_drain : Bool
      @pending.lock &.requested = true
      @publish_confirm_requested.try_send true
      true
    rescue ::Channel::ClosedError
      false
    end

    # Blocks until the drain numbered *seq* by #copy_drain, or a later one,
    # has synced. Returns without it at shutdown and while sync is disabled,
    # also when a reload disables it during the wait: the caller checks again.
    def wait_drain(seq : UInt64) : Nil
      return if drain_synced?(seq) || !Config.instance.sync?
      waiter = ::Channel(Nil).new
      @drain_waiters.lock &.push({seq, waiter})
      return if drain_synced?(seq) # a drain released the waiters meanwhile
      return unless request_drain
      waiter.receive?
    end

    # Block until everything written so far is durable, on the leader and on
    # the in-sync followers. Used by transaction commits, which also have to
    # persist their acks, so it syncs the whole filesystem.
    def sync : Nil
      waiter = ::Channel(Nil).new
      @pending.lock &.waiters.push(waiter)
      begin
        @publish_confirm_requested.try_send true
      rescue ::Channel::ClosedError
        # The loop has exited (shutdown), and its final drain may have run
        # before our waiter was added. Its fd may be closed by now.
        File.open(@data_dir) { |dir| FileSystem.syncfs(dir.fd) } if Config.instance.sync?
        return
      end
      waiter.receive?
    end

    # Syncs this node's data dir now, without a drain or a wait for followers.
    # Run before the stores load: a store doesn't know which of its messages
    # an earlier process copied to other queues, and those copies can still be
    # in the page cache, so they're made durable before any segment unlink.
    def sync_local : Nil
      FileSystem.syncfs(@data_dir_fd) if Config.instance.sync?
      {% unless flag?(:release) %}
        @local_syncs += 1
      {% end %}
    end

    def close : Nil
      @publish_confirm_requested.close
    end

    private def publish_confirm_loop
      loop do
        # Wake on the first request, then sync + confirm everything pending.
        # While syncing, new requests accumulate in @pending and are flushed
        # by the next iteration — batching emerges without any delay.
        @publish_confirm_requested.receive
        drain
      end
    rescue ::Channel::ClosedError
      # @publish_confirm_requested is closed; flush anything that was persisted
      # but not yet confirmed before exiting.
      drain
      # Nothing syncs after this, the waiters see their drain unsynced
      release_drain_waiters
      LibC.close(@data_dir_fd) if @data_dir_fd >= 0
    end

    private def drain : Nil
      batch = nil
      seq = 0u64
      @pending.replace do |current|
        if current.drainable?
          batch = current
          seq = @drains_started += 1
          Batch.new
        else
          current
        end
      end
      return unless batch

      # Clear the flags before syncing: a write after this re-registers the
      # file for the next drain, instead of being missed by this one
      batch.files.each &.clear_needs_msync!
      dirs = Set(String).new
      batch.files.each { |f| dirs << File.dirname(f.path) if f.take_created! }
      sync = Config.instance.sync?
      syncfs = batch.syncfs? || !batch.waiters.empty? || @skipped_sync ||
               batch.files.size + batch.paths.size + dirs.size > Config.instance.syncfs_threshold
      paths = batch.files.map(&.path).concat(batch.paths).concat(dirs) unless syncfs
      replicator = @replicator
      if replicator
        # Requested before our own sync so the followers persist and ack
        # while it runs. Buffered for each follower's flush fiber to write:
        # this loop runs on an isolated thread and must never write the
        # follower sockets itself, their fds belong to the default execution
        # context's event loop (see Follower#flush_loop).
        if paths
          replicator.request_fsync(paths)
        else
          replicator.request_syncfs
        end
        replicator.followers.each &.request_flush
      end
      begin
        if sync
          if paths
            syncfs = fsync_paths(batch.files, batch.paths, dirs)
          else
            syncfs_data_dir
          end
        end
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
      {% unless flag?(:release) %}
        if sync
          record = SyncRecord.new(syncfs, paths || Array(String).new)
          @last_sync = record
          @syncs_lock.synchronize { @syncs << record }
        end
      {% end %}

      # Block until every in-sync follower has acked the replicated bytes
      # (having synced what was requested) and any ISR shrink is committed
      # to the coordinator, so a confirm means the data is durable on the
      # leader and on every node that could be promoted on failover. While
      # the coordinator is unreachable confirms stall (publishers time out,
      # message state stays uncertain — never falsely confirmed), and if it
      # stays unreachable the leader's lease expires and the process exits.
      replicator.try &.wait_for_followers
      if sync
        @skipped_sync = false
        @drains_synced.set(seq, :release)
        @drain_waiters.lock do |waiters|
          waiters.reject! do |waited, waiter|
            waiter.close if waited <= seq
            waited <= seq
          end
        end
      else
        @skipped_sync = true
        # No drain syncs while sync is disabled, so nothing waits for one
        release_drain_waiters
      end

      batch.acks.each do |target, id|
        target.enqueue_confirm_ack(id)
      end
      batch.waiters.each &.close
    end

    private def release_drain_waiters : Nil
      @drain_waiters.lock do |waiters|
        waiters.each { |_, waiter| waiter.close }
        waiters.clear
      end
    end

    # Returns whether it fell back to syncing the whole filesystem
    private def fsync_paths(files, paths, dirs) : Bool
      files.each &.fsync
      paths.each do |path|
        File.open(path, &.fsync)
      rescue File::NotFoundError
      end
      dirs.each do |dir|
        File.open(dir, &.fsync)
      rescue File::NotFoundError
      end
      false
    rescue ex : IO::Error | RuntimeError
      raise ex unless FileSystem.out_of_file_descriptors?(ex)
      # Opening a path to fsync it needs a free descriptor, and so does the
      # event loop the first open on this thread creates. The data dir fd is
      # already open, so sync everything through it rather than exit: out of
      # descriptors is a condition the broker is supposed to ride out.
      Log.warn { "Out of file descriptors, syncing the whole data dir instead: #{ex.message}" }
      syncfs_data_dir
      true
    end

    private def syncfs_data_dir : Nil
      FileSystem.syncfs(@data_dir_fd)
    end
  end
end
