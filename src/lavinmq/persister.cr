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
  # sync then msyncs exactly those files, so a publish confirm doesn't also
  # write out dirty pages of unrelated files (ack files, other vhosts) the
  # way a whole-filesystem syncfs did.
  class Persister
    Log = LavinMQ::Log.for "persister"

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

    def initialize(@replicator : Clustering::Replicator? = nil)
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

    # Make all writes since the last sync durable on this node and on every
    # in-sync follower. Used by tx.commit, so commit-ok carries the same
    # durability guarantee as a publish confirm: the data is msynced here and
    # fsynced (via the dispatched fsync requests) on every node that could be
    # promoted on failover.
    def sync : Nil
      sync_dirty_files
      @replicator.try &.wait_for_followers
    end

    def close : Nil
      @publish_confirm_requested.close
    end

    private def publish_confirm_loop
      loop do
        # Wake on the first request, then sync + confirm everything pending.
        # While msync runs, new requests accumulate in @pending_acks and are
        # flushed by the next iteration — batching emerges without any delay.
        @publish_confirm_requested.receive
        drain_pending_acks
      end
    rescue ::Channel::ClosedError
      # @publish_confirm_requested is closed; flush anything that was persisted
      # but not yet confirmed before exiting.
      drain_pending_acks
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
      dirty.each do |mfile|
        next if mfile.closed? || mfile.deleted?
        begin
          mfile.fsync
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

    private def drain_pending_acks
      acks : Hash(AMQP::Channel, UInt64)? = nil
      @pending_acks.replace do |current|
        if current.empty?
          current
        else
          acks = current
          Hash(AMQP::Channel, UInt64).new
        end
      end
      return unless acks

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

      acks.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end
  end
end
