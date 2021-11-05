require "../segment_position"

module AvalancheMQ
  class Queue
    # MessageIndex
    # New messages are written to "enq.#{sp.segment}"
    # Deleting msg from index appends to "ack.#{sp.segment}"
    # When enq.X and ack.X are of equal size they can both be deleted
    # ack.X files are not sorted but enq.X are (naturally)
    # Requeued messages are put in a Dequeue (kept sorted)
    # When shifting messages from the MessageIndex the Requeue is emptied first
    # Then the enq.X files are read in order
    # All ack.X entries are read at start and kept in a sorted array @acked
    # When reading from enq.X make sure they aren't already in @acked
    class ReadyQueue
      @lock = Mutex.new(:reentrant)
      getter count = 0

      def initialize(queue_name)
        @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
        @enq = Hash(UInt32, MFile).new do |h, seg|
          h[seg] = MFile.new(File.join(@index_dir, "enq.#{seg}"), capacity: 8 * 1024 * 1024).tap do |file|
              file.seek(0, IO::Seek::End)
          end
        end
        @ack = Hash(UInt32, MFile).new do |h, seg|
          h[seg] = MFile.new(File.join(@index_dir, "ack.#{seg}"), capacity: 8 * 1024 * 1024).tap do |file|
              file.seek(0, IO::Seek::End)
          end
        end
        load_enq_files_in_order
        @acked = load_acked
        @requeued = Deque(SegmentPosition).new
      end

      private def load_enq_files_in_order : Nil
        files = Dir.children(@index_dir).select! &.starts_with?("enq.")
        segs = files.map(&.split('.', 2).last.to_i).sort!
        segs.each { |seg| @enq[seg] }
      end

      private def load_acked : Array(SegmentPosition)
        arr = Array(SegmentPosition).new
        Dir.new(@index_dir).each do |name|
          next unless name.starts_with?("ack.")
          File.open(File.join(@index_dir, name)) do |f|
            loop do
              arr << f.read_bytes SegmentPosition
            rescue IO::EOFError
              break
            end
          end
        end
        arr.sort!
      end

      # Enqueue a new SegmentPosition, do not use for requeuing
      def enqueue(sp : SegmentPosition)
        @enq[sp.segment].write_bytes sp
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

      def shift?
        @lock.synchronize do
          sp = @requeued.shift? || next_enqued
          @count -= 1 if sp
          sp
        end
      end

      private def next_enqued : SegmentPosition?
        @enq.each do |seg, mfile|
          loop do
            sp = mfile.read_bytes(SegmentPosition)
            return sp unless sp == @acked.first?

            @acked.shift
          rescue IO::EOFError
            @enq.delete(seg)
            break
          end
        end
      end

      def shift
        shift? || raise IndexError.new
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
        @ack[sp.segment].write_bytes sp
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        until @count < size
          sp = shift? || break
          yield sp
        end
      end

      def limit_byte_size(bytesize, &blk : SegmentPosition -> Nil)
        raise NotImplemented.new
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
