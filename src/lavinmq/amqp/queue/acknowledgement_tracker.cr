require "../../unacked_message"
require "../../config"

module LavinMQ::AMQP
  class AcknowledgementTracker
    @unacked_count = Atomic(UInt32).new(0u32)
    @unacked_bytesize = Atomic(UInt64).new(0u64)
    @unacked_count_log = Deque(UInt32).new(Config.instance.stats_log_size)

    getter basic_get_unacked = Deque(UnackedMessage).new
    getter unacked_count_log

    def unacked_count
      @unacked_count.get(:relaxed)
    end

    def unacked_bytesize
      @unacked_bytesize.get(:relaxed)
    end

    def add_unacked(bytesize : UInt64)
      @unacked_count.add(1, :relaxed)
      @unacked_bytesize.add(bytesize, :relaxed)
    end

    def remove_unacked(bytesize : UInt64)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(bytesize, :relaxed)
    end
  end
end
