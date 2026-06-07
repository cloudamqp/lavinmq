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
    # Confirm acks accumulated since the last drain. Durability is decided at
    # drain time against the in-sync set as it exists then (see
    # #replicated_to_followers?), which is safe because a follower only reaches
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

      unless replicated_to_followers?
        begin
          sync
        rescue ex
          Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
          exit 1
        end
      end

      acks.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end

    # True if the pending acks are durable on followers, so the local syncfs can be
    # skipped. We wait for every currently in-sync follower: a follower that
    # acks has the replicated bytes; one that disconnects while we wait simply
    # leaves the in-sync set (and won't be promoted on failover), so it no
    # longer needs the data. Durable as long as at least one in-sync follower
    # remains and has it. Only when there are no in-sync followers at all (or
    # we're standalone) do we fall back to syncfs.
    private def replicated_to_followers? : Bool
      replicator = @replicator || return false
      followers = replicator.followers
      return false if followers.empty?
      # Every follower still in the ISR must have the data: any of them may be
      # promoted to leader on failover. wait_for_confirm blocks until the
      # follower acks (true) or drops out of the ISR by disconnecting (false),
      # so on return every follower still in the ISR has necessarily acked.
      # Wait for all of them (no short-circuit) — a disconnect just removes that
      # one from the ISR; we stay durable as long as at least one remains. Only
      # an emptied-out ISR falls back to syncfs.
      acked = false
      followers.each { |f| acked = true if f.wait_for_confirm }
      acked
    end
  end
end
