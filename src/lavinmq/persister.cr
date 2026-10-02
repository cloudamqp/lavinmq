require "./config"
require "./logger"
require "./mfile"
require "./filesystem"
require "./clustering/replicator"
require "./clustering/follower"
require "sync/exclusive"

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
      # Callers of #sync, released by the drain that synced for them
      getter waiters = Array(::Channel(Nil)).new

      def drainable? : Bool
        !acks.empty? || !waiters.empty?
      end
    end

    {% unless flag?(:release) %}
      record SyncRecord, syncfs : Bool, paths : Array(String)
      # Spec instrumentation: what the most recent drain synced
      getter last_sync : SyncRecord?
    {% end %}

    @data_dir_fd : Int32 = -1
    @publish_confirm_requested = ::Channel(Bool).new(1)
    # Acks, dirty files and sync waiters share one lock, so a drain swaps out
    # every file marked before the acks it confirms
    @pending = Sync::Exclusive(Batch).new(Batch.new, :unchecked)
    # Milliseconds since @epoch when the ongoing sync started, 0 when not
    # syncing. Polled by the watchdog, so the drain never waits on it and a
    # dead watchdog can't wedge confirms.
    @sync_started = Atomic(Int64).new(0)
    @epoch = Time.instant
    @closed = Atomic(Bool).new(false)

    def initialize(@data_dir : String, @replicator : Clustering::Replicator? = nil)
      @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
      raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      # Run on a dedicated thread so the blocking syscalls only stall this
      # thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
      # Isolated too, as a stalled device can block every default context
      # thread in file I/O or page faults
      Fiber::ExecutionContext::Isolated.new("Sync watchdog") { sync_watchdog_loop }
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

    # Like mark_dirty(MFile), for regular (non-mmapped) files
    def mark_dirty(path : String) : Nil
      @pending.lock &.paths.add(path)
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

    def close : Nil
      @closed.set(true, :release)
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
      LibC.close(@data_dir_fd) if @data_dir_fd >= 0
    end

    private def sync_watchdog_loop : Nil
      reported_start = 0i64
      reports = 0
      until @closed.get(:acquire)
        sleep sync_timeout / 4
        started = @sync_started.get(:acquire)
        next if started.zero?
        if started != reported_start
          reported_start = started
          reports = 0
        end
        elapsed = (now_ms - started).milliseconds
        next if elapsed < sync_timeout * (reports + 1)
        reports += 1
        begin
          sync_stalled(elapsed)
        rescue
          # e.g. the log output failed, keep watching
        end
      end
    end

    private def now_ms : Int64
      (Time.instant - @epoch).total_milliseconds.to_i64 + 1
    end

    # Called once per timeout a sync stays blocked, as a follower may finish
    # its full sync while the disk is still stalled
    protected def sync_stalled(elapsed : Time::Span) : Nil
      if @replicator.try &.in_sync_followers?
        Log.fatal { "Disk sync blocked for #{elapsed.total_seconds.to_i}s, exiting so a follower can take over" }
        exit 1
      end
      Log.error { "Disk sync blocked for #{elapsed.total_seconds.to_i}s, no in-sync follower to fail over to" }
    end

    protected def sync_timeout : Time::Span
      Config.instance.clustering_sync_timeout
    end

    private def watched(&) : Nil
      @sync_started.set(now_ms, :release)
      begin
        yield
      ensure
        @sync_started.set(0, :release)
      end
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
      syncfs = !batch.waiters.empty? ||
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
          watched { paths ? fsync_paths(batch.files, batch.paths, dirs) : syncfs_data_dir }
        end
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
      {% unless flag?(:release) %}
        @last_sync = SyncRecord.new(syncfs, paths || Array(String).new)
      {% end %}

      # Block until every in-sync follower has acked the replicated bytes
      # (having synced what was requested) and any ISR shrink is committed
      # to the coordinator, so a confirm means the data is durable on the
      # leader and on every node that could be promoted on failover. While
      # the coordinator is unreachable confirms stall (publishers time out,
      # message state stays uncertain — never falsely confirmed), and if it
      # stays unreachable the leader's lease expires and the process exits.
      replicator.try &.wait_for_followers

      batch.acks.each do |target, id|
        target.enqueue_confirm_ack(id)
      end
      batch.waiters.each &.close
    end

    private def fsync_paths(files, paths, dirs) : Nil
      files.each &.fsync
      paths.each do |path|
        File.open(path, &.fsync)
      rescue File::NotFoundError
      end
      dirs.each do |dir|
        File.open(dir, &.fsync)
      rescue File::NotFoundError
      end
    end

    private def syncfs_data_dir : Nil
      FileSystem.syncfs(@data_dir_fd)
    end
  end
end
