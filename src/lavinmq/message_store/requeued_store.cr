require "deque"
require "../segment_position"

module LavinMQ
  class MessageStore
    abstract class RequeuedStore
      abstract def shift? : SegmentPosition?
      abstract def first? : SegmentPosition?
      abstract def insert(sp : SegmentPosition) : Nil
      abstract def clear : Nil
    end

    class OrderedRequeuedStore < RequeuedStore
      @segment_positions = Deque(SegmentPosition).new

      def shift? : SegmentPosition?
        @segment_positions.shift?
      end

      def first? : SegmentPosition?
        @segment_positions.first?
      end

      def insert(sp : SegmentPosition) : Nil
        if idx = @segment_positions.bsearch_index { |rsp| rsp > sp }
          @segment_positions.insert(idx, sp)
        else
          @segment_positions.push(sp)
        end
      end

      def clear : Nil
        @segment_positions = Deque(SegmentPosition).new
      end
    end
  end
end
