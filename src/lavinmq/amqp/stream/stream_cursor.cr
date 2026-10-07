require "../../segment_position"
require "../../rough_time"
require "./filters/consumer_filter"

module LavinMQ::AMQP
  # A read position in a stream, created by StreamMessageStore#cursor and
  # read with StreamMessageStore#shift?. The store pins the cursor's segment
  # from the first shift until #close, see StreamMessageStore#unmap_if_unused.
  #
  # Must only be used under the lock guarding the store, except for reading
  # #offset and #caught_up?, which may be stale without it.
  class StreamCursor
    getter offset : Int64
    getter segment : UInt32
    getter pos : UInt32
    getter segment_since = RoughTime.instant # when it moved into its segment
    getter requeued = Deque(SegmentPosition).new
    property? pinned = false
    getter? closed = false

    def initialize(@store : StreamMessageStore, @offset : Int64, @segment : UInt32, @pos : UInt32,
                   @filter : ConsumerFilter? = nil)
    end

    def requeue(sp : SegmentPosition) : Nil
      @requeued.push(sp)
    end

    def caught_up? : Bool
      @offset > @store.last_offset && @requeued.empty?
    end

    def match?(headers : AMQP::Table?) : Bool
      filter = @filter
      filter.nil? || filter.match?(headers)
    end

    # Releases the pin, a closed cursor reads nothing more
    def close : Nil
      return if @closed
      @closed = true
      @store.unpin(self)
    end

    # Steps past the message at the current position, used by the store
    def advance(bytesize : UInt32) : Nil
      @pos += bytesize
      @offset += 1
    end

    # Moves to the start of `segment`, used by the store
    def enter(segment : UInt32) : Nil
      @segment = segment
      @pos = 4u32
      @segment_since = RoughTime.instant
    end
  end
end
