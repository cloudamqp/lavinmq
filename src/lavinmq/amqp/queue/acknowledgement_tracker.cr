require "../../unacked_message"
require "../../config"

module LavinMQ::AMQP
  class AcknowledgementTracker
    @unacked_count = Atomic(UInt32).new(0u32)
    @unacked_bytesize = Atomic(UInt64).new(0u64)

    getter basic_get_unacked = Deque(UnackedMessage).new

    def unacked_count
      @unacked_count.get(:relaxed)
    end

    def unacked_bytesize
      @unacked_bytesize.get(:relaxed)
    end

    def track_unacked(bytesize : UInt64)
      @unacked_count.add(1, :relaxed)
      @unacked_bytesize.add(bytesize, :relaxed)
    end

    def untrack_unacked(bytesize : UInt64)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(bytesize, :relaxed)
    end
  end
end
