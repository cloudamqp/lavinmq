require "../../../message_store"
require "../../../message_store/requeued_store"
require "../../../min_heap"
require "../queue"

module LavinMQ::AMQP
  abstract class DelayedQueue < Queue
    class DelayedMessageStore < MessageStore
      class DelayedRequeuedStore < MessageStore::RequeuedStore
        record DelayedSegmentPosition,
          sp : SegmentPosition,
          expire_at : Int64 do
          include Comparable(self)

          def <=>(other : self)
            r = expire_at <=> other.expire_at
            return r unless r.zero?
            sp <=> other.sp
          end
        end

        @segment_positions = MinHeap(DelayedSegmentPosition).new

        def shift? : SegmentPosition?
          @segment_positions.shift?.try &.sp
        end

        def first? : SegmentPosition?
          @segment_positions.first?.try &.sp
        end

        # Whether the first message is due at `now`, by the time it was inserted
        # with, which is later than its own delay when it's delayed again, see
        # DelayedMessageStore#delay_again
        def first_due?(now : Int64) : Bool
          if first = @segment_positions.first?
            first.expire_at <= now
          else
            false
          end
        end

        def time_to_next_expiration? : Time::Span?
          sp = @segment_positions.first?
          return if sp.nil?
          Queue.time_to_expiration_wakeup(sp.expire_at)
        end

        def insert(sp : SegmentPosition) : Nil
          raise "BUG: this insert overload should not be called"
        end

        def insert(sp : SegmentPosition, timestamp : Int64) : Nil
          @segment_positions.push(DelayedSegmentPosition.new(sp, timestamp + sp.delay))
        end

        def size
          @segment_positions.size
        end

        def clear : Nil
          @segment_positions.clear
        end
      end
    end
  end
end
