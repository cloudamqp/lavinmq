require "../segment_position"

module AvalancheMQ
  class VHost
    class ReferencedSPs
      include Enumerable(SegmentPosition)

      def initialize(initial_capacity = 128)
        @pq = PriorityQueue(SPQueue).new(initial_capacity)
      end

      def <<(q : SPQueue)
        @pq << q unless q.empty?
      end

      def empty?
        @pq.empty?
      end

      def each
        pq = @pq
        until pq.empty?
          q = pq.shift
          yield q.shift
          pq.push q unless q.empty?
        end
      end
    end
  end
end
