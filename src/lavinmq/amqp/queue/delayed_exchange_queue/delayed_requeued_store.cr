require "../../../message_store/requeued_store"

module LavinMQ::AMQP
  class DelayedExchangeQueue < Queue
    class DelayedMessageStore < MessageStore
      class DelayedRequeuedStore < MessageStore::RequeuedStore
        record DelayedSegmentPosition,
          sp : SegmentPosition,
          expire_at : Int64

        @segment_positions = Deque(DelayedSegmentPosition).new

        def shift? : SegmentPosition?
          @segment_positions.shift?.try &.sp
        end

        def first? : SegmentPosition?
          @segment_positions.first?.try &.sp
        end

        def time_to_next_expiration? : Time::Span?
          sp = @segment_positions.first?
          return if sp.nil?
          (sp.expire_at - RoughTime.unix_ms).milliseconds
        end

        def insert(sp : SegmentPosition) : Nil
          raise "BUG: should not be called"
        end

        def insert(sp : SegmentPosition, timestamp : Int64) : Nil
          sp = DelayedSegmentPosition.new(sp, timestamp + sp.delay)
          idx = @segment_positions.bsearch_index do |rsp|
            if rsp.expire_at == sp.expire_at
              rsp.sp > sp.sp
            else
              rsp.expire_at > sp.expire_at
            end
          end

          if idx
            @segment_positions.insert(idx, sp)
          else
            @segment_positions.push(sp)
          end
        end

        def clear : Nil
          @segment_positions = Deque(DelayedSegmentPosition).new
        end
      end
    end
  end
end
