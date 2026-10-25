require "../btree_set"
require "../segment_position"

module LavinMQ
  class MessageStore
    abstract class RequeuedStore
      abstract def shift? : SegmentPosition?
      abstract def first? : SegmentPosition?
      # Returns false if `sp` was already in the store
      abstract def insert(sp : SegmentPosition) : Bool
      abstract def size
      abstract def clear : Nil
    end

    class PublishOrderedRequeuedStore < RequeuedStore
      @segment_positions = BTreeSet(SegmentPosition).new

      def shift? : SegmentPosition?
        @segment_positions.shift?
      end

      def first? : SegmentPosition?
        @segment_positions.first?
      end

      def insert(sp : SegmentPosition) : Bool
        @segment_positions.insert(sp)
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
