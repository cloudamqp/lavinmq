require "../../segment_position"

module LavinMQ::AMQP
  # The offset a stream consumer that tracks its offset resumes at: its first
  # delivered message that isn't settled (acked, or rejected without requeue),
  # or, when all are settled, the offset its cursor has read to. A requeued
  # message stays unsettled until it's settled after its redelivery.
  #
  # First deliveries come in offset order, which is segment position order, so
  # the unsettled deliveries stay sorted and a settled one is found by binary
  # search. The buffer grows to the consumer's prefetch and is then reused.
  #
  # Thread safe: the deliver loop records deliveries while the client's acks
  # settle them.
  class StreamAckTracker
    private record Delivery, sp : SegmentPosition, offset : Int64, settled : Bool = false

    @deliveries = Deque(Delivery).new
    @lock = Mutex.new
    @last_resume_offset : Int64? = nil

    def initialize(@read_to : Int64)
    end

    # A first delivery of the message at `offset`, before it's sent
    def delivered(sp : SegmentPosition, offset : Int64) : Nil
      @lock.synchronize do
        @deliveries.push Delivery.new(sp, offset)
        @read_to = offset + 1
      end
    end

    # The cursor read to `offset` without delivering, past filtered out messages
    def read_to(offset : Int64) : Nil
      @lock.synchronize { @read_to = offset }
    end

    # Settles the delivery of `sp` and yields the resume offset if that moved.
    # Yields under the lock, so offsets are stored in the order they're found.
    def settle(sp : SegmentPosition, & : Int64 -> _) : Nil
      @lock.synchronize do
        idx = @deliveries.bsearch_index { |d| d.sp >= sp } || return
        delivery = @deliveries[idx]
        return unless delivery.sp == sp
        @deliveries[idx] = delivery.copy_with(settled: true)
        while @deliveries.first?.try &.settled
          @deliveries.shift
        end
        offset = @deliveries.first?.try(&.offset) || @read_to
        return if offset == @last_resume_offset
        @last_resume_offset = offset
        yield offset
      end
    end
  end
end
