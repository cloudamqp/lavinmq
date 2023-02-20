require "./priority_queue"
require "./spqueue"
require "../segment_position"

module LavinMQ
  class VHost
    class ReferencedSPs
      include Enumerable(SegmentPosition)

      def initialize(initial_capacity = 128)
        @pq = PriorityQueue(SPQueue).new(initial_capacity)
      end

      def <<(q : SPQueue)
        unless q.empty?
          q.lock
          @pq << q
        end
      end

      def empty?
        @pq.empty?
      end

      def each(&)
        pq = @pq
        until pq.empty?
          q = pq.shift
          yield q.shift
          if q.empty?
            q.unlock
          else
            pq.push q
          end
        end
      ensure
        pq = @pq
        pq.size.times do
          pq.shift.unlock
        end
      end

      class NotInOrderError < Exception
        def initialize(prev_sp : SegmentPosition, sp : SegmentPosition)
          super("SPs not in order. prev_sp=#{prev_sp} sp=#{sp}")
        end
      end
    end
  end
end
