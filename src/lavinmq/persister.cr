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

    # One confirm batch: the pending acks plus the durability requirement
    # captured when the batch started (its first ack was enqueued). A batch may
    # skip syncfs only if it was replicated to and acked by the followers that
    # were in-sync at that point; otherwise it falls back to local syncfs.
    private class Batch
      getter acks = Hash(AMQP::Channel, UInt64).new
      # Followers that were in-sync when the batch started (nil until captured).
      property followers : Array(Clustering::Follower)? = nil
      # Sync generation at capture; a mismatch at drain means a follower joined
      # the in-sync set mid-batch and we must fall back to syncfs.
      property generation = 0_i64
      # No in-sync follower (or standalone) when the batch started → must syncfs.
      property? needs_sync = false

      def empty?
        @acks.empty?
      end
    end

    @data_dir_fd : Int32 = -1
    @publish_confirm_requested = ::Channel(Bool).new(1)
    @pending : Sync::Exclusive(Batch) = Sync::Exclusive.new(Batch.new, :unchecked)

    def initialize(data_dir : String, @replicator : Clustering::Replicator? = nil)
      @data_dir_fd = LibC.open(data_dir.check_no_null_byte, LibC::O_RDONLY)
      raise IO::Error.from_errno("Failed to open #{data_dir}") if @data_dir_fd < 0
      # Run on a dedicated thread so the blocking syncfs(2) syscall only stalls
      # this thread, not the worker threads handling client connections.
      Fiber::ExecutionContext::Isolated.new("Publish confirm loop") { publish_confirm_loop }
    end

    def enqueue_ack(channel : AMQP::Channel, msgid : UInt64)
      if Config.instance.sync?
        @pending.lock do |batch|
          # On the first ack of a new batch, bind the batch to the in-sync set
          # that exists right now (i.e. before/at this message's write). A
          # follower that becomes synced later isn't credited — its incremental
          # stream wouldn't include this batch's earlier writes.
          if batch.empty?
            if replicator = @replicator
              fs, generation = replicator.in_sync_followers
              if fs.empty?
                batch.needs_sync = true # not replicated to any synced follower
              else
                batch.followers = fs
                batch.generation = generation
              end
            else
              batch.needs_sync = true # standalone: only local syncfs makes it durable
            end
          end
          batch.acks[channel] = msgid
        end
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
      batch : Batch? = nil
      @pending.replace do |current|
        if current.empty?
          current
        else
          batch = current
          Batch.new
        end
      end
      return unless b = batch

      begin
        # Skip the (slow) local syncfs only if the batch is durable on followers:
        # the in-sync set captured at batch start is unchanged (no follower
        # joined since), and every one of those followers acked the replicated
        # bytes. In every other case (standalone, no synced followers, a mid-
        # batch join, or a follower that disconnected before acking) fall back
        # to syncfs.
        durable = false
        unless b.needs_sync?
          if (fs = b.followers) && !fs.empty? &&
             @replicator.try(&.synced_generation) == b.generation
            durable = fs.all? &.wait_for_confirm
          end
        end
        sync unless durable
      rescue ex
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end

      b.acks.each do |channel, msgid|
        channel.enqueue_confirm_ack(msgid)
      end
    end
  end
end
