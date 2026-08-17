require "./config"
require "./logger"
require "./amqp/channel"
require "./clustering/replicator"
require "./clustering/follower"
require "sync/exclusive"

module LavinMQ
  # Owns the filesystem fd used for syncfs(2) and the publish-confirm
  # batching loop. A single Persister is created per Server and shared
  # between all VHosts — since they live on the same filesystem, one
  # syncfs flushes data for every vhost in one syscall.
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
    @syncfs_lock = Mutex.new # serialize all syncfs syscalls, and guard @closed/@syncfs_ok/@data_dir_fd teardown
    getter? closed = false

    def initialize(data_dir : String, @replicator : Clustering::Replicator? = nil)
      @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
      raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      @syncfs_ok = Channel(Nil).new(2) # 2 slots: one for syncfs start, one for syncfs end
      # Run on a dedicated thread so the blocking syncfs(2) syscall only stalls
      # this thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
      # Timeout and exit process if syncfs doesn't respond in time
      Fiber::ExecutionContext::Isolated.new("syncfs timeout loop") { syncfs_timeout_loop }
    end

    private def syncfs_timeout_loop
      loop do
        @syncfs_ok.receive # syncfs is about to run
        wait_for_syncfs
      rescue Channel::ClosedError
        break
      end
    end

    private def wait_for_syncfs : Nil
      select
      when @syncfs_ok.receive     # syncfs completed
      when timeout syncfs_timeout # syncfs blocked for too long
        unless @replicator
          # No follower to fail over to — dying doesn't help availability, it
          # just turns a slow disk into a full outage. Log and keep waiting.
          Log.error { "syncfs(2) is blocked" }
          @syncfs_ok.receive # wait for the real completion so it isn't later
          # mistaken for the *next* call's start signal (see #sync)
          return
        end
        Log.fatal { "syncfs(2) is blocked, exiting so a follower can take over" }
        exit 1
      end
    end

    protected def syncfs_timeout : Time::Span
      Config.instance.clustering_syncfs_timeout
    end

    # Every confirm — sync, no-sync, and clustered alike — is routed through the
    # publish confirm loop so each channel has exactly one producer of ack
    # frames. `sync` is a runtime-mutable INI option (SIGHUP reloads it); taking
    # a shortcut here when it is disabled would let a later direct ack overtake
    # an earlier batched one after a mid-stream flip, sending cumulative
    # Basic.Ack frames out of delivery-tag order (see #2078). The loop skips the
    # actual syncfs while sync is disabled (see drain_pending_acks), so no-sync
    # only pays a single hop to the loop, not a disk flush.
    def enqueue_ack(channel : AMQP::Channel, msgid : UInt64)
      @pending_acks.lock { |acks| acks[channel] = msgid }
      @publish_confirm_requested.try_send true
    rescue ::Channel::ClosedError
    end

    def sync : Nil
      @syncfs_lock.synchronize do
        # Persister is closing (or closed) on another fiber: @syncfs_ok and
        # @data_dir_fd may already be torn down (see #close), so bail out
        # instead of sending on a closed channel or syncing a closed fd.
        return if @closed
        @syncfs_ok.send nil
        begin
          sync_data_dir
        ensure
          @syncfs_ok.send nil
        end
      end
    end

    protected def sync_data_dir : Nil
      {% if flag?(:linux) %}
        ret = LibC.syncfs(@data_dir_fd)
        raise IO::Error.from_errno("syncfs") if ret != 0
      {% else %}
        LibC.sync
      {% end %}
    end

    def close : Nil
      @publish_confirm_requested.close
    end

    private def publish_confirm_loop
      loop do
        # Wake on the first request, then sync + confirm everything pending.
        # While syncfs runs, new requests accumulate in @pending_acks and are
        # flushed by the next iteration — batching emerges without any delay.
        @publish_confirm_requested.receive
        drain_pending_acks
      end
    rescue ::Channel::ClosedError
      # @publish_confirm_requested is closed; flush anything that was persisted
      # but not yet confirmed before exiting.
      drain_pending_acks
      # Take the same lock `sync` uses so this can't race a concurrent (or
      # about-to-start) sync() call: either it's already in flight and holds
      # the lock — we wait for it to finish before tearing anything down —
      # or it hasn't started yet and will see @closed and bail out below.
      @syncfs_lock.synchronize do
        @closed = true
        LibC.close(@data_dir_fd) if @data_dir_fd >= 0
        @syncfs_ok.close # close the syncfs timeout loop
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

      # Ask each follower's flush fiber to push the pending replicated bytes,
      # so they persist and ack them while our own syncfs runs. Only a
      # request: this loop runs on an isolated thread and must never write
      # the follower sockets itself — their fds belong to the default
      # execution context's event loop (see Follower#flush_loop).
      @replicator.try &.followers.each &.request_flush
      if Config.instance.sync?
        begin
          sync
        rescue ex
          Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
          exit 1
        end
      end
      # Block until every in-sync follower has acked the replicated bytes and
      # any ISR shrink is committed to the coordinator, so a confirm means
      # the data is durable on the leader (syncfs, done above) and on every
      # node that could be promoted on failover. While the coordinator is
      # unreachable confirms stall (publishers time out, message state stays
      # uncertain — never falsely confirmed), and if it stays unreachable the
      # leader's lease expires and the process exits.
      @replicator.try &.wait_for_followers

      acks.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end
  end
end
