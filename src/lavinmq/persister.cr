require "./config"
require "./logger"
require "./amqp/channel"
require "./clustering/replicator"
require "./clustering/follower"
require "./mfile"
require "sync/exclusive"

module LavinMQ
  # Owns the dirty segment file set and the publish-confirm batching loop.
  # A single Persister is created per Server and shared between all VHosts.
  # Message stores register each segment file they write to (mark_dirty);
  # sync then msyncs exactly those files. Large batches fall back to one
  # filesystem-wide sync because many individual msync calls can cost more
  # than syncfs; the cutoff is configurable with `syncfs_threshold`.
  class Persister
    Log = LavinMQ::Log.for "persister"

    @data_dir_fd : Int32 = -1
    @publish_confirm_requested = ::Channel(Bool).new(1)
    # Confirm acks accumulated since the last drain. The follower set is
    # decided at drain time against the in-sync set as it exists then (see
    # Clustering::Server#wait_for_followers), which is safe because a
    # follower only reaches the in-sync set after a full_sync that includes
    # every prior write.
    @pending_acks : Sync::Exclusive(Hash(AMQP::Channel, UInt64)) = Sync::Exclusive.new(Hash(AMQP::Channel, UInt64).new, :unchecked)
    # Files written since the last sync. Each file registers itself only on
    # its first write after a sync (see MFile#mark_needs_msync!), so this
    # stays a handful of entries per drain.
    @dirty_files : Sync::Exclusive(Array(MFile)) = Sync::Exclusive.new(Array(MFile).new, :unchecked)
    # Fibers blocked in #sync (tx.commit), each waiting on its own channel to
    # be closed by the drain that made their writes durable.
    @sync_waiters : Sync::Exclusive(Array(::Channel(Nil))) = Sync::Exclusive.new(Array(::Channel(Nil)).new, :unchecked)

    def initialize(@replicator : Clustering::Replicator? = nil, *, data_dir : String = Config.instance.data_dir)
      {% if flag?(:linux) %}
        @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
        raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      {% end %}
      # Run on a dedicated thread so the blocking msync(2) syscalls only stall
      # this thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
    end

    # Every confirm — sync, no-sync, and clustered alike — is routed through the
    # publish confirm loop so each channel has exactly one producer of ack
    # frames. `sync` is a runtime-mutable INI option (SIGHUP reloads it); taking
    # a shortcut here when it is disabled would let a later direct ack overtake
    # an earlier batched one after a mid-stream flip, sending cumulative
    # Basic.Ack frames out of delivery-tag order (see #2078). The loop skips the
    # actual msync while sync is disabled (see sync_dirty_files), so no-sync
    # only pays a single hop to the loop, not a disk flush.
    def enqueue_ack(channel : AMQP::Channel, msgid : UInt64)
      @pending_acks.lock { |acks| acks[channel] = msgid }
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
    end

    # Register a segment file to be msynced by the next sync. Hot path: one
    # atomic swap per message; the lock is only taken the first time a file
    # is dirtied after a sync. Callers must mark only after dispatching the
    # write to the replicator, so that a marked write is on the wire before
    # the `$` fsync request that has to cover it (see sync_dirty_files).
    def mark_dirty(mfile : MFile) : Nil
      return if mfile.mark_needs_msync!
      @dirty_files.lock { |files| files << mfile }
    end

    # Block until all writes so far are durable on this node and on every
    # in-sync follower. Used by tx.commit, so commit-ok carries the same
    # durability guarantee as a publish confirm — and it's released by the
    # same drain: the publish confirm loop is the only thread that msyncs,
    # so tx commits never sync concurrently with it, they wait for it.
    def sync : Nil
      waiter = ::Channel(Nil).new
      @sync_waiters.lock { |waiters| waiters << waiter }
      @publish_confirm_requested.try_send true
      waiter.receive?
    rescue ::Channel::ClosedError
      # Persister closed (shutdown); the loop thread is gone, so syncing
      # inline can't race it. Only the wake above raises — a waiter the final
      # drain picked up is signaled by close, not by an exception — so the
      # writes still need to be made durable here.
      sync_dirty_files
      @replicator.try &.wait_for_followers
    end

    def close : Nil
      @publish_confirm_requested.close
    end

    private def publish_confirm_loop
      loop do
        # Wake on the first request, then sync + release everything pending.
        # While msync runs, new requests accumulate in @pending_acks and
        # @sync_waiters and are flushed by the next iteration — batching
        # emerges without any delay.
        @publish_confirm_requested.receive
        drain
      end
    rescue ::Channel::ClosedError
      # @publish_confirm_requested is closed; flush anything that was persisted
      # but not yet confirmed before exiting.
      drain
    ensure
      {% if flag?(:linux) %}
        LibC.close(@data_dir_fd) if @data_dir_fd >= 0
        @data_dir_fd = -1
      {% end %}
    end

    private def sync_dirty_files : Nil
      dirty : Array(MFile)? = nil
      @dirty_files.replace do |current|
        if current.empty?
          current
        else
          dirty = current
          Array(MFile).new
        end
      end
      return unless dirty

      # Clear the flags before dispatching the fsync requests below: a write
      # marked after its file's clear re-registers it for the next sync, and
      # one marked before was dispatched to the stream even earlier (see
      # MessageStore#write_to_disk), so it's on the wire ahead of this batch's
      # `$` records. Either way no write can slip between a fsync request and
      # the ack the confirm waits for.
      dirty.each &.clear_needs_msync!

      # Ask the followers to fsync the same files, so a follower ack past
      # these requests means the data is durable there too. Dispatched before
      # our own msync so followers sync in parallel with us. Only a request:
      # this may run on an isolated thread that must never write the follower
      # sockets itself (see Follower#request_fsync).
      if replicator = @replicator
        paths = dirty.reject(&.deleted?).map(&.path)
        replicator.fsync_files(paths) unless paths.empty?
      end
      return unless Config.instance.sync?
      begin
        sync_files(dirty)
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
    end

    # Flush small batches file by file to avoid writing unrelated dirty data.
    # Once the number of live files reaches the configured threshold, one
    # syncfs is faster than issuing many serial msync calls on typical storage.
    protected def sync_files(dirty : Array(MFile)) : Nil
      live_count = dirty.count { |mfile| !mfile.closed? && !mfile.deleted? }
      if live_count >= Config.instance.syncfs_threshold
        syncfs
        return
      end

      dirty.each do |mfile|
        next if mfile.closed? || mfile.deleted?
        begin
          sync_file(mfile)
        rescue IO::Error
          # Closed in the window since the check above — a deleted segment's
          # content no longer needs durability. A real msync failure raises an
          # errno-based RuntimeError, handled below.
        rescue ex
          Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
          exit 1
        end
      end
    end

    protected def sync_file(mfile : MFile) : Nil
      mfile.fsync
    end

    protected def syncfs : Nil
      {% if flag?(:linux) %}
        ret = LibC.syncfs(@data_dir_fd)
        raise IO::Error.from_errno("syncfs") if ret != 0
      {% else %}
        LibC.sync
      {% end %}
    end

    private def drain
      acks : Hash(AMQP::Channel, UInt64)? = nil
      @pending_acks.replace do |current|
        if current.empty?
          current
        else
          acks = current
          Hash(AMQP::Channel, UInt64).new
        end
      end
      waiters : Array(::Channel(Nil))? = nil
      @sync_waiters.replace do |current|
        if current.empty?
          current
        else
          waiters = current
          Array(::Channel(Nil)).new
        end
      end
      return unless acks || waiters

      sync_dirty_files
      # Ask each follower's flush fiber to push the pending replicated bytes,
      # so they persist and ack them while our own msync runs. Only a
      # request: this loop runs on an isolated thread and must never write
      # the follower sockets itself — their fds belong to the default
      # execution context's event loop (see Follower#flush_loop).
      @replicator.try &.followers.each &.request_flush
      # Block until every in-sync follower has acked the replicated bytes
      # (including the fsync requests dispatched above) and any ISR shrink is
      # committed to the coordinator, so a confirm means the data is durable
      # on the leader (msync, done above) and on every node that could be
      # promoted on failover. While the coordinator is unreachable confirms
      # stall (publishers time out, message state stays uncertain — never
      # falsely confirmed), and if it stays unreachable the leader's lease
      # expires and the process exits.
      @replicator.try &.wait_for_followers

      # Everything is durable: unblock the tx commits first (their connection
      # fibers send the CommitOk themselves), then hand the confirm acks to
      # the channels' confirm writer fibers.
      waiters.try &.each &.close
      acks.try &.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end
  end
end
