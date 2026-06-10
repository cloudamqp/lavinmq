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
    # #wait_for_followers), which is safe because a follower only reaches
    # the in-sync set after a full_sync that includes every prior write.
    @pending_acks : Sync::Exclusive(Hash(AMQP::Channel, UInt64)) = Sync::Exclusive.new(Hash(AMQP::Channel, UInt64).new, :unchecked)

    def initialize(data_dir : String, @replicator : Clustering::Replicator? = nil)
      @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
      raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      # Run on a dedicated thread so the blocking syncfs(2) syscall only stalls
      # this thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
    end

    def enqueue_ack(channel : AMQP::Channel, msgid : UInt64)
      if Config.instance.sync?
        @pending_acks.lock { |acks| acks[channel] = msgid }
        @publish_confirm_requested.try_send true
      else
        # If sync is disabled, we can confirm immediately without waiting for
        # the publish confirm loop to flush to disk.
        channel.enqueue_confirm_ack(msgid)
      end
    rescue ::Channel::ClosedError
    end

    def sync : Nil
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
      LibC.close(@data_dir_fd) if @data_dir_fd >= 0
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

      # Push the pending replicated bytes to the followers first, so they
      # persist and ack them while our own syncfs runs.
      followers = @replicator.try &.followers
      followers.try &.each &.flush
      begin
        sync
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end
      wait_for_followers(followers)

      acks.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end

    # Block until every in-sync follower has acked the replicated bytes, so a
    # confirm means the data is durable on the leader (syncfs, done before
    # this) and on every follower that could be promoted on failover. Wait
    # for all of them (no short-circuit) — wait_for_confirm blocks until the
    # follower acks or disconnects.
    #
    # A follower that disconnected (wait_for_confirm == false, or it dropped
    # before the drain and left the ISR dirty) may lack data that's about to
    # be confirmed, so its removal from the ISR must be *committed to the
    # coordinator* before the confirm goes out — otherwise a leader crash
    # right after the confirm could elect that follower and lose the
    # confirmed messages. flush_isr retries until the write succeeds: while
    # the coordinator is unreachable confirms stall (publishers time out,
    # message state stays uncertain — never falsely confirmed), and if it
    # stays unreachable the leader's lease expires and the process exits.
    private def wait_for_followers(followers) : Nil
      all_acked = true
      followers.try &.each { |f| all_acked &= f.wait_for_confirm }
      if replicator = @replicator
        replicator.flush_isr if !all_acked || replicator.isr_dirty?
      end
    end
  end
end
