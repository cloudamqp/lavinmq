require "../segment_position"

module AvalancheMQ
  class Queue
    # ReadyQueue is a sorted Deque of SegmentPositions
    class ReadyQueue
      @lock = Mutex.new(:reentrant)
      getter count = 0

      def initialize(queue_name)
        @enq = MFile.new(File.join(@index_dir, "enq"), capacity: 1024 * 1024 )
        @ack = MFile.new(File.join(@index_dir, "ack"), capacity: 1024 * 1024 )
        @requeued = Deque(SegmentPosition).new
      end

      # Enqueue a new SegmentPosition, do not use for requeuing
      def enqueue(sp : SegmentPosition)
        @lock.synchronize do
          @enq.write_bytes sp
        end
      end

      # insert a SP, keeps the deque sorted
      # returns SPs in the deque after the operation
      def requeue(sp : SegmentPosition)
        @lock.synchronize do
          if i = @requeued.bsearch_index { |rsp| rsp > sp }
            @requeued.insert(i, sp)
          else
            @requeued.push(sp)
          end
          @count += 1
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def requeue(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          sps.reverse_each do |sp|
            if i = @requeued.bsearch_index { |rsp| rsp > sp }
              @requeued.insert(i, sp)
            else
              @requeued.push(sp)
            end
            @count += 1
          end
          @count
        end
      end

      def shift
        shift? || raise IndexError.new
      end

      def shift?
        @lock.synchronize do
          sp = @requeued.shift?
          sp ||= @enq.read_bytes(SegmentPosition) if @enq.pos + SP_SIZE <= @enq.size
          @count -= 1 if sp
          sp
        end
      end

      # Shift until block breaks or it returns false
      # If broken with false yield, return the message to the queue
      def shift(& : SegmentPosition -> Bool)
        @lock.synchronize do
          while sp = shift?
            ok = yield sp
            unless ok
              reque(sp)
              break
            end
          end
        end
      end

      # Deletes a SP somewhere in the deque
      # returns true/false whether found
      def ack(sp) : Bool
        @ack.write_bytes sp
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @ready.size > size
            sp = @ready.shift? || break
            yield sp
          end
        end
      end

      def limit_byte_size(bytesize, &blk : SegmentPosition -> Nil)
        @lock.synchronize do
          while @ready.sum(&.bytesize) > bytesize
            sp = @ready.shift? || break
            yield sp
          end
        end
      end

      def empty?
        @requeued.empty? && @enq.pos == @enq.size
      end

      # yields all messages, then clears it
      # returns number of messages in the queue before purge
      def purge
        @lock.synchronize do
          count = @ready.size
          if @ready.capacity == @inital_capacity
            @ready.clear
          else
            @ready = Deque(SegmentPosition).new(@inital_capacity)
            GC.collect
          end
          count
        end
      end

      def size
        @ready.size
      end

      def capacity
        @ready.capacity
      end

      def sum(&blk : SegmentPosition -> _) : UInt64
        @ready.sum(0_u64, &blk)
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
    end

    abstract class SortedReadyQueue < ReadyQueue
      def push(sp : SegmentPosition) : Int32
        insert(sp)
      end

      def insert(sp : SegmentPosition)
        @lock.synchronize do
          insert_sorted(sp)
          @ready.size
        end
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def insert(sps : Enumerable(SegmentPosition))
        @lock.synchronize do
          sps.each do |sp|
            insert_sorted(sp)
          end
          @ready.size
        end
      end
    end

    class ExpirationReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        idx = @ready.bsearch_index do |rsp|
          rsp.expiration_ts > sp.expiration_ts
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end

    class PriorityReadyQueue < SortedReadyQueue
      private def insert_sorted(sp)
        idx = @ready.bsearch_index do |rsp|
          rsp.priority < sp.priority
        end
        idx ? @ready.insert(idx, sp) : @ready.push(sp)
      end
    end
  end
end
