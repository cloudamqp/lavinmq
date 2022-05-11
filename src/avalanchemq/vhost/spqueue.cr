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
        SPUnsortedReadyQueue.new(ready)
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

    class SPUnsortedReadyQueue < SPQueue
      @list : Array(SegmentPosition)

      # FIXME: This could be a performance issue on long queues
      # that is using Delayed messages or Priority
      # We need to sort the ready queue by SP for the GC as a
      # SortedReadyQueue could be sorted on any attribute on a SP
      def initialize(ready : Queue::SortedReadyQueue)
        @list = ready.to_a.sort! { |a, b| b <=> a }
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

    class SPUnackQueue < SPQueue
      def initialize(@unack : Queue::UnackQueue)
      end

      def peek : SegmentPosition
        @unack[@pos].sp
      end

      def shift : SegmentPosition
        v = @unack[@pos].sp
        @pos += 1
        v
      end

      def empty? : Bool
        @pos == @unack.size
      end

      def lock : Nil
        @unack.lock
      end

      def unlock : Nil
        @unack.unlock
      end
    end
  end
end
