module AvalancheMQ
  class VHost
    class PriorityQueue(T)
      def initialize(initial_capacity)
        @queue = Deque(T).new(initial_capacity)
      end

      def empty?
        @queue.empty?
      end

      def shift
        @queue.shift
      end

      def size
        @queue.size
      end

      def push(item : T)
        q = @queue
        if idx = q.bsearch_index { |e| e > item }
          q.insert(idx, item)
        else
          q.push item
        end
      end

      def <<(item : T)
        push(item)
      end
    end
  end
end
