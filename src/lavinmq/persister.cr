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
  #
  # It also unlinks the segments that the message stores have emptied, after
  # a drain (see #unlink_after_sync): the broker moves messages between
  # queues (dead-lettering, delays, retries) by writing a copy and acking
  # the original, and the unlink of the original's segment must not become
  # durable before the copy. The stores only mark the copies dirty and hand
  # over the files, so the order is kept here, in one place, for as long as
  # the process lives, whatever happens to the store that emptied them.
  class Persister
    Log = LavinMQ::Log.for "persister"

    # Receives publish confirms once the confirmed data is durable
    module ConfirmTarget
      # The 1-slot mailbox a writer fiber of the target drains, nil until the
      # target has asked for confirms
      abstract def confirm_mailbox : ::Channel(UInt64)?

      # Called from the Persister's thread with the highest persisted id.
      # Non-blocking: ids are cumulative, so a stale one still waiting in the
      # mailbox is replaced.
      def enqueue_confirm_ack(msgid : UInt64) : Nil
        mailbox = confirm_mailbox || return
        loop do
          return if mailbox.try_send(msgid)
          mailbox.try_receive?
        end
      rescue ::Channel::ClosedError
      end
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
      # Emptied segments to unlink once this batch is synced, and the callers
      # of #sync_marked, released after those unlinks
      getter unlinks = Array(Unlink).new
      getter unlink_waiters = Array(::Channel(Nil)).new

      def drainable? : Bool
        !acks.empty? || !waiters.empty? || !files.empty? || !paths.empty? || after_sync?
      end

      def after_sync? : Bool
        !unlinks.empty? || !unlink_waiters.empty?
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

    # An emptied segment, with its ack file, see #unlink_after_sync
    record Unlink, segment : MFile, meta_path : String, acks : MFile?
    # What a synced batch leaves for the unlink loop
    private record Synced, unlinks : Array(Unlink), waiters : Array(::Channel(Nil))

    {% unless flag?(:release) %}
      record SyncRecord, syncfs : Bool, paths : Array(String)
      # Spec instrumentation: what the latest drains synced, oldest first
      @syncs = Array(SyncRecord).new
      @syncs_lock = Mutex.new

      def syncs : Array(SyncRecord)
        @syncs_lock.synchronize { @syncs.dup }
      end

      def last_sync : SyncRecord?
        @syncs_lock.synchronize { @syncs.last? }
      end
    {% end %}

    # The files written before this process started, or while sync was
    # disabled, aren't marked dirty, so the next drain that something is
    # unlinked after syncs the whole filesystem
    @unmarked_writes = true
    # The directories with files, or that are themselves, still to be
    # unlinked, with how many, see #flush_unlinks
    @unlinking = Sync::Exclusive(Hash(String, Int32)).new(Hash(String, Int32).new(0), :unchecked)
    # Unbounded, so a drain never waits for the unlink loop, which can
    # block on a follower
    @synced = Sync::Exclusive(Deque(Synced)).new(Deque(Synced).new, :unchecked)
    @synced_added = ::Channel(Bool).new(1)
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
      # The unlinks are replicated, and only the default execution context
      # may write to the follower sockets (see Follower#flush_loop)
      spawn(name: "Persister unlink loop") { unlink_loop }
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

    # Requests a drain of what is marked dirty, without waiting for it
    def drain_soon : Nil
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
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

    # Unlinks an emptied segment and its ack file once everything marked
    # dirty so far is durable, here and on the in-sync followers, without
    # blocking the caller. The caller must have dropped its references to
    # the files, a delivery can still hold a lease on the mapping.
    def unlink_after_sync(segment : MFile, meta_path : String, acks : MFile?) : Nil
      unlink = Unlink.new(segment, meta_path, acks)
      # Counted under the lock, so a #flush_unlinks that sees it queues its
      # waiter behind the unlink
      @pending.lock do |batch|
        unlinking(File.dirname(segment.path))
        batch.unlinks.push(unlink)
      end
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
      # The loop has exited (shutdown). Its final drain took the unlink
      # unless it's still pending.
      if unlink && @pending.lock &.unlinks.delete(unlink)
        syncfs_closed
        perform(unlink)
      end
    end

    # Blocks until everything marked dirty so far is durable and the files
    # handed to #unlink_after_sync so far are unlinked. Unlike #sync it
    # only syncs the whole filesystem when there can be unmarked writes.
    def sync_marked : Nil
      waiter = ::Channel(Nil).new
      @pending.lock &.unlink_waiters.push(waiter)
      begin
        @publish_confirm_requested.try_send true
      rescue ::Channel::ClosedError
        return syncfs_closed
      end
      waiter.receive?
    end

    # Blocks until what was handed to #unlink_after_sync in *dir*, and below
    # it, is unlinked. Before a store loads a directory, or a declared queue
    # reuses one, that another store instance emptied segments in: they are
    # unlinked by their paths. Unlinks elsewhere aren't waited for.
    def flush_unlinks(dir : String) : Nil
      pending = @unlinking.lock do |dirs|
        next false if dirs.empty?
        below = "#{dir}/"
        dirs.each_key.any? { |d| d == dir || d.starts_with?(below) }
      end
      sync_marked if pending
    end

    def close : Nil
      @publish_confirm_requested.close
    end

    private def unlinking(dir : String) : Nil
      @unlinking.lock { |dirs| dirs[dir] += 1 }
    end

    private def unlinked(dir : String) : Nil
      @unlinking.lock do |dirs|
        dirs.delete(dir) if (dirs[dir] -= 1) <= 0
      end
    end

    # After the loop has exited its fd may be closed
    private def syncfs_closed : Nil
      File.open(@data_dir) { |dir| FileSystem.syncfs(dir.fd) } if Config.instance.sync?
    end

    private def unlink_loop : Nil
      open = true
      while open
        open = !@synced_added.receive?.nil?
        while synced = @synced.lock &.shift?
          synced.unlinks.each { |unlink| perform(unlink) }
          synced.waiters.each &.close
        end
      end
    end

    private def perform(unlink : Unlink) : Nil
      # The segment first: a crash in between leaves an ack file without
      # its segment, which the next start removes, not a segment without
      # its acks, whose messages would come back
      delete_file(unlink.segment, unlink.meta_path)
      unlink.acks.try { |acks| delete_file(acks) }
    rescue ex
      Log.error(exception: ex) { "Failed to unlink #{unlink.segment.path}" }
    ensure
      unlinked(File.dirname(unlink.segment.path))
    end

    private def delete_file(file : MFile, meta_path : String? = nil) : Nil
      file.delete(raise_on_missing: false)
      if replicator = @replicator
        replicator.delete_file(meta_path) if meta_path
        replicator.delete_file(file.path)
      end
      File.delete?(meta_path) if meta_path
      # A delivery may still be reading from the mapping, see Envelope#lease
      file.close
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
      @synced_added.close
      LibC.close(@data_dir_fd) if @data_dir_fd >= 0
    end

    private def drain : Nil
      batch = nil
      @pending.replace do |current|
        if current.drainable?
          batch = current
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
      syncfs = batch.syncfs? || !batch.waiters.empty? ||
               (@unmarked_writes && batch.after_sync?) ||
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
        if Config.instance.sync?
          if paths
            syncfs = fsync_paths(batch.files, batch.paths, dirs)
          else
            syncfs_data_dir
          end
          @unmarked_writes = false if syncfs
        else
          @unmarked_writes = true # the marks this batch took are gone
        end
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
      {% unless flag?(:release) %}
        record = SyncRecord.new(syncfs, paths || Array(String).new)
        @syncs_lock.synchronize do
          @syncs.shift if @syncs.size >= 1024 # debug builds run for long
          @syncs << record
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

      if batch.after_sync?
        @synced.lock &.push(Synced.new(batch.unlinks, batch.unlink_waiters))
        begin
          @synced_added.try_send true
        rescue ::Channel::ClosedError
        end
      end
      batch.acks.each do |target, id|
        target.enqueue_confirm_ack(id)
      end
      batch.waiters.each &.close
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
