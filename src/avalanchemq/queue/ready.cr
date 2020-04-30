require "../segment_position"

module AvalancheMQ
  class Queue
    # ReadyQueue is a sorted Deque of SegmentPositions
    class ReadyQueue
      @lock = Mutex.new(:reentrant)

      def initialize(capacity = 8)
        @ready = Deque(SegmentPosition).new(capacity)
      end

      def shift
        @lock.synchronize do
          @ready.shift
        end
      end

      def shift?
        @lock.synchronize do
          @ready.shift?
        end
      end

      # Shift until block breaks or it returns false
      # If broken with false yield, return the message to the queue
      def shift(&blk : SegmentPosition -> Bool)
        @lock.synchronize do
          while sp = @ready.shift?
            ok = yield sp
            unless ok
              @ready.unshift sp
              break
            end
          end
        end
      end

      # Yields an iterator over all SPs, the deque is locked
      # while it's being read from
      def with_all(&blk : Iterator(SegmentPosition) -> Nil)
        @lock.synchronize do
          yield @ready.each
        end
      end

      # Iterate over all SPs in the deque, locking while reading
      def each(&blk)
        @lock.synchronize do
          @ready.each { |sp| yield sp }
        end
      end

      def locked_each(&blk)
        @lock.synchronize do
          yield @ready.each
        end
      end

      # insert a SP, keeps the deque sorted
      # returns SPs in the deque after the operation
      def insert(sp : SegmentPosition)
        @lock.synchronize do
          i = @ready.bsearch_index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          sps.reverse_each do |sp|
            i = @ready.bsearch_index { |rsp| rsp > sp } || 0
            @ready.insert(i, sp)
          end
          @ready.size
        end
      end

      # Deletes a SP somewhere in the deque
      # returns true/false whether found
      def delete(sp) : Bool
        return false if @ready.empty?
        @lock.synchronize do
          if @ready.first == sp
            @ready.shift
            return true
          else
            if idx = @ready.bsearch_index { |rsp| rsp >= sp }
              if @ready[idx] == sp
                @ready.delete_at(idx)
                return true
              end
            end
          end
        end
        false
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        while @ready.size > size
          sp = @ready.shift? || break
          yield sp
        end
      end

      # Pushes a SP to the end of the deque
      # Returns number of SPs in the deque
      def push(sp : SegmentPosition) : Int32
        @lock.synchronize do
          @ready.push(sp)
          @ready.size
        end
      end

      # alias for `push`
      def <<(sp)
        push(sp)
      end

      def first?
        @ready[0]?
      end

      def empty?
        @ready.empty?
      end

      # yields all messages, then clears it
      # returns number of messages in the queue before purge
      def purge
        @lock.synchronize do
          count = @ready.size
          @ready.each { |sp| yield sp }
          @ready.clear
          count
        end
      end

      def size
        @ready.size
      end

      def capacity
        @ready.capacity
      end

      def copy_to(set)
        @lock.synchronize do
          @ready.each do |sp|
            set << sp
          end
        end
      end
    end
  end
end
