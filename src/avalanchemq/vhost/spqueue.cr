require "../queue"

module AvalancheMQ
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

      def <=>(other : self)
        peek <=> other.peek
      end

      abstract def peek : SegmentPosition
      abstract def shift : SegmentPosition
      abstract def empty? : Bool
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
    end
  end
end
