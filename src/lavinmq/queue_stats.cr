require "./stats"

module LavinMQ::AMQP
  module QueueStats
    include Stats

    macro included
      @unacked_count = Atomic(UInt32).new(0u32)
      @unacked_bytesize = Atomic(UInt64).new(0u64)

      rate_stats(
        {"ack", "deliver", "deliver_no_ack", "deliver_get", "confirm", "get", "get_no_ack", "publish", "redeliver", "reject", "return_unroutable", "dedup"},
        {"message_count", "unacked_count"})
    end

    def unacked_count
      @unacked_count.get(:relaxed)
    end

    def unacked_bytesize
      @unacked_bytesize.get(:relaxed)
    end

    def queue_stats_details
      unacked_count = unacked_count()
      unacked_bytesize = unacked_bytesize()
      unacked_avg_bytes = unacked_count.zero? ? 0u64 : unacked_bytesize//unacked_count

      {
        unacked:                      unacked_count,
        messages_unacknowledged:      unacked_count,
        unacked_bytes:                unacked_bytesize,
        message_bytes_unacknowledged: unacked_bytesize,
        unacked_avg_bytes:            unacked_avg_bytes,
      }
    end

    private def reset_queue_stats
      @unacked_count.set(0u32, :relaxed)
      @unacked_bytesize.set(0u64, :relaxed)
    end
  end
end
