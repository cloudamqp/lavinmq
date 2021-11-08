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
    #
    # new message => enq.X
    # deliver message => @unacked deque
    # acked message => ack.X (delete ack.X and enq.X if of equal size and not last/current)
    # rejected message => @requeued deque
    # next? => @requeud || enq (unless == ack.first)
    class ReadyQueue
      getter bytesize = 0i64
      getter count = 0i32
      @acked : Deque(SegmentPosition)
      @requeued = Deque(SegmentPosition).new

      def initialize(@index_dir : String)
        FileUtils.mkdir_p @index_dir
        @ack = Hash(UInt32, MFile).new do |h, seg|
          h[seg] = MFile.new(File.join(@index_dir, "ack.#{seg}"), capacity: 8 * 1024 * 1024)
        end
        @acked = load_acked
        @enq = Hash(UInt32, MFile).new do |h, seg|
          h[seg] = MFile.new(File.join(@index_dir, "enq.#{seg}"), capacity: 8 * 1024 * 1024)
        end
        scan_enqueued
      end

      private def scan_enqueued : Nil
        acked = @acked
        files = Dir.children(@index_dir).select! &.starts_with?("enq.")
        segs = files.map(&.split('.', 2).last.to_u32).sort!
        segs.each do |seg|
          count = 0
          bytesize = 0i64
          enq = @enq[seg]
          loop do
            sp = enq.read_bytes SegmentPosition
            if sp == acked.first?
              acked.rotate!
              next
            end
            count += 1
            bytesize += sp.bytesize
          rescue IO::EOFError
            break
          end
          enq.pos = 0
          @bytesize += bytesize
          @count += count
        end
      end

      private def load_acked : Deque(SegmentPosition)
        acked = Deque(SegmentPosition).new
        Dir.new(@index_dir).each do |name|
          next unless name.starts_with?("ack.")
          MFile.open(File.join(@index_dir, name)) do |f|
            loop do
              acked << f.read_bytes SegmentPosition
            rescue IO::EOFError
              break
            end
          end
        end
        acked.sort!
      end

      def first? : SegmentPosition?
        sp = @requeued.first? || first_enqued?
        @count -= 1 if sp
        sp
      end

      private def first_enqued? : SegmentPosition?
        @enq.each_value do |mfile|
          loop do
            pos = mfile.pos
            sp = mfile.read_bytes(SegmentPosition)
            if sp == @acked.first?
              @acked.shift
            else
              mfile.pos = pos
              return sp
            end
          rescue IO::EOFError
            break
          end
        end
      end

      # Enqueue a new SegmentPosition, do not use for requeuing
      def enqueue(sp : SegmentPosition)
        enq = @enq[sp.segment]
        enq.seek(0, IO::Seek::End) do
          enq.write_bytes sp
        end
        @bytesize += sp.bytesize
        @count += 1
      end

      # insert a SP, keeps the deque sorted
      # returns SPs in the deque after the operation
      def requeue(sp : SegmentPosition)
        if i = @requeued.bsearch_index { |rsp| rsp > sp }
          @requeued.insert(i, sp)
        else
          @requeued.push(sp)
        end
        @count += 1
      end

      # Insert SPs sorted, the array should ideally be sorted too
      def requeue(sps : Enumerable(SegmentPosition))
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

      def shift?
        sp = @requeued.shift? || next_enqued
        @count -= 1 if sp
        sp
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
          while sp = shift?
            ok = yield sp
            unless ok
              requeue(sp)
              break
            end
          end
      end

      def each(& : SegmentPosition -> Nil)
        STDERR.puts "WARNING, MessageIndex#each is destructive"
        while sp = shift?
          yield sp
        end
      end

      def ack(sp)
        ack = @ack[sp.segment]
        ack.write_bytes sp
        return if @ack.last_key == sp.segment

        enq = @enq[sp.segment]
        if ack.size == enq.size
          @ack.delete(sp.segment)
          @enq.delete(sp.segment)
          ack.delete
          ack.close
          enq.delete
          enq.close
        end
      end

      def limit_size(size, &blk : SegmentPosition -> Nil)
        until @count < size
          sp = shift? || break
          yield sp
        end
      end

      def limit_byte_size(bytesize, &blk : SegmentPosition -> Nil)
        until @bytesize < bytesize
          sp = shift? || break
          yield sp
        end
      end

      def empty?
        @requeued.empty? && @enq.last_value?.try &.pos == @ack.last_value?.try &.size
      end

      # yields all messages, then clears it
      # returns number of messages in the queue before purge
      def purge
        @enq.each_value &.close
        @enq.clear
        @ack.each_value &.close
        @ack.clear
        FileUtils.rm_rf @index_dir
        FileUtils.mkdir @index_dir
        count = @count
        @count = 0
        @bytesize = 0
        count
      end

      def size
        @count
      end

      def capacity
        @acked.@capacity + @requeued.@capacity
      end

      def compact
      end
    end
  end
end
