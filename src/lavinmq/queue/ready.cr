require "../segment_position"

module LavinMQ
  class Queue
    # ReadyQueue is a sorted Deque of SegmentPositions
    class ReadyQueue
      @lock = Mutex.new
      @initial_capacity : Int32
      getter empty_change = Channel(Bool).new
      getter bytesize = 0u64

      def initialize(initial_capacity = 1024)
        @initial_capacity = initial_capacity.to_i32
        @ready = Deque(SegmentPosition).new(@initial_capacity)
      end

      private def notify_empty(is_empty)
        while @empty_change.try_send is_empty
        end
      end

      def includes?(sp)
        @ready.includes?(sp)
      end

      def shift
        @lock.synchronize do
          sp = @ready.shift
          @bytesize -= sp.bytesize
          sp
        end
      end

      def shift?
        @lock.synchronize do
          if sp = @ready.shift?
            @bytesize -= sp.bytesize
            sp
          else
            notify_empty(true)
          end
        end
      end

      # Shift until block breaks or it returns false
      # If broken with false yield, return the message to the queue
      def shift(&blk : SegmentPosition -> Bool)
        @lock.synchronize do
          loop do
            sp = @ready.shift? || return notify_empty(true)
            ok = yield sp
            unless ok
              @ready.unshift sp
              break
            end
            @bytesize -= sp.bytesize
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

      def each(start : Int, count : Int, &blk)
        @lock.synchronize do
          @ready.each(start: start, count: count) { |sp| yield sp }
        end
      end

      def locked_each(&blk)
        @lock.synchronize do
          yield @ready.each
        end
      end

      def bsearch_index(&blk)
        @lock.synchronize do
          @ready.bsearch_index { |sp, i| yield sp, i }
        end
      end

      # insert a SP, keeps the deque sorted
      # returns SPs in the deque after the operation
      def insert(sp : SegmentPosition)
        @lock.synchronize do
          was_empty = @ready.size.zero?
          if i = @ready.bsearch_index { |rsp| rsp > sp }
            @ready.insert(i, sp)
          else
            @ready.push(sp)
          end
          notify_empty(false) if was_empty
          @bytesize += sp.bytesize
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          was_empty = @ready.size.zero?
          sps.reverse_each do |sp|
            if i = @ready.bsearch_index { |rsp| rsp > sp }
              @ready.insert(i, sp)
            else
              @ready.push(sp)
            end
            @bytesize += sp.bytesize
          end
          notify_empty(false) if was_empty
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
            @bytesize -= sp.bytesize
            notify_empty(true) if @ready.empty?
            return true
          else
            if idx = @ready.bsearch_index { |rsp| rsp >= sp }
              if @ready[idx] == sp
                @ready.delete_at(idx)
                @bytesize -= sp.bytesize
                notify_empty(true) if @ready.empty?
                return true
              end
            end
          end
        end
        false
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @ready.size > size
            sp = @ready.shift? || break
            @bytesize -= sp.bytesize
            yield sp
          end
          notify_empty(true) if @ready.empty?
        end
      end

      def limit_byte_size(bytesize, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @bytesize > bytesize
            sp = @ready.shift? || break
            @bytesize -= sp.bytesize
            yield sp
          end
          notify_empty(true) if @ready.empty?
        end
      end

      # Pushes a SP to the end of the deque
      # Returns number of SPs in the deque
      def push(sp : SegmentPosition) : Int32
        @lock.synchronize do
          was_empty = @ready.empty?
          @ready.push(sp)
          @bytesize += sp.bytesize
          notify_empty(false) if was_empty
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

      def [](idx)
        @ready[idx]
      end

      def []?(idx)
        @ready[idx]?
      end

      def empty?
        @ready.empty?
      end

      # yields all messages, then clears it
      # returns number of messages in the queue before purge
      def purge
        @lock.synchronize do
          count = @ready.size
          if @ready.capacity == @initial_capacity
            @ready.clear
          else
            @ready = Deque(SegmentPosition).new(@initial_capacity)
          end
          @bytesize = 0u64
          notify_empty(true)
          count
        end
      end

      def size
        @ready.size
      end

      def capacity
        @ready.capacity
      end

      def compact
        @lock.synchronize do
          @ready = Deque(SegmentPosition).new(@ready.size) { |i| @ready[i] }
        end
      end

      def lock
        @lock.lock
      end

      def unlock
        @lock.unlock
      end

      def to_a
        @ready.to_a
      end

      def avg_bytesize
        return 0u64 if @ready.size.zero?
        @bytesize // @ready.size
      end

      # expensive calculation used for ready queue details
      def max_bytesize(&blk : SegmentPosition -> _) : UInt32
        return 0u32 if @ready.size.zero?
        @ready.max_of(&blk)
      end

      # expensive calculation used for ready queue details
      def min_bytesize(&blk : SegmentPosition -> _) : UInt32
        return 0u32 if @ready.size.zero?
        @ready.min_of(&blk)
      end
    end

    abstract class SortedReadyQueue < ReadyQueue
      def push(sp : SegmentPosition) : Int32
        insert(sp)
      end

      def insert(sp : SegmentPosition)
        @lock.synchronize do
          was_empty = @ready.empty?
          insert_sorted(sp)
          notify_empty(false) if was_empty
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          was_empty = @ready.empty?
          sps.each do |sp|
            insert_sorted(sp)
          end
          notify_empty(false) if was_empty
          @ready.size
        end
      end
    end

    class ExpirationReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        @bytesize += sp.bytesize
        idx = @ready.bsearch_index do |rsp|
          rsp.expiration_ts > sp.expiration_ts
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end

    class PriorityReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        @bytesize += sp.bytesize
        idx = @ready.bsearch_index do |rsp|
          rsp.priority < sp.priority
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end
  end
end
