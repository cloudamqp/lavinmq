require "../queue"

module LavinMQ
  class VHost
    abstract class SPQueue
      include Comparable(self)
      @pos = 0

      def self.new(ready : Queue::ReadyQueue)
        SPReadyQueue.new(ready)
      end

      def self.new(unack : Queue::UnackQueue)
        SPUnackQueue.new(unack)
      end

      def self.new(ready : Queue::SortedReadyQueue)
        SPUnsortedQueue.new(ready)
      end

      def self.new(sps : Array(SegmentPosition))
        SPUnsortedQueue.new(sps)
      end

      def <=>(other : self)
        peek <=> other.peek
      end

      abstract def peek : SegmentPosition
      abstract def shift : SegmentPosition
      abstract def empty? : Bool
      abstract def lock : Nil
      abstract def unlock : Nil
    end

    class SPReadyQueue < SPQueue
      def initialize(@ready : Queue::ReadyQueue)
      end

      def peek : SegmentPosition
        @ready[@pos]
      end

      def shift : SegmentPosition
        v = @ready[@pos]
        @pos += 1
        v
      end

      def empty? : Bool
        @pos == @ready.size
      end

      def lock : Nil
        @ready.lock
      end

      def unlock : Nil
        @ready.unlock
      end
    end

    class SPUnsortedQueue < SPQueue
      @list : Array(SegmentPosition)

      # FIXME: This could be a performance issue on long queues
      # that is using Delayed messages or Priority
      # We need to sort the ready queue by SP for the GC as a
      # SortedReadyQueue could be sorted on any attribute on a SP
      def initialize(ready : Queue::SortedReadyQueue)
        @list = ready.to_a.sort! { |a, b| b <=> a }
      end

      def initialize(@list : Array(SegmentPosition))
        @list.sort! { |a, b| b <=> a }
      end

      def peek : SegmentPosition
        @list.last
      end

      def shift : SegmentPosition
        @list.pop
      end

      def empty? : Bool
        @list.empty?
      end

      def lock : Nil
      end

      def unlock : Nil
      end
    end
  end
end
