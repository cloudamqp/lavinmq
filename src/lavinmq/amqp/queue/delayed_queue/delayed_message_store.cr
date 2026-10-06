require "../../../message_store"
require "./delayed_requeued_store"

module LavinMQ::AMQP
  abstract class DelayedQueue < Queue
    # A delayed queue must have its messages ordered by "expire at" so the
    # message expire loop will look at the right message. To achieve this,
    # messages are always added to a custom requeued store, which acts as an
    # in-memory index ordered by when messages should be published.
    # The requeued store is used because #shift and #first? look for requeued
    # messages first, then read the next from disk. For a delayed queue we
    # never want to read messages in the order they arrived (were written to disk).
    class DelayedMessageStore < MessageStore
      # Redefine @requeued (defined in MessageStore)
      @requeued : RequeuedStore = DelayedRequeuedStore.new

      private def requeued : DelayedRequeuedStore
        @requeued.as(DelayedRequeuedStore)
      end

      # Customization used by DelayedExchangeQueue
      def time_to_next_expiration? : Time::Span?
        requeued.time_to_next_expiration?
      end

      def initialize(*args, **kwargs)
        super
        build_index
      end

      def build_index
        @log.debug { "rebuilding delayed index (#{@size} messages)" }
        # Unfortunately we have to read all messages and build an "index"
        requeued = DelayedRequeuedStore.new
        count = 0u32
        bytesize = 0u64
        @segments.each do |seg_id, mfile|
          mfile.pos = 4
          loop do
            pos = mfile.pos.to_u32
            break if pos == mfile.size
            if deleted?(seg_id, pos)
              BytesMessage.skip(mfile)
              next
            end
            ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
            break if ts.zero? # This means that the rest of the file is zero, so break.
            msg = BytesMessage.from_bytes(mfile.to_slice + pos)
            sp = SegmentPosition.make(seg_id, pos, msg)
            mfile.seek(sp.bytesize, IO::Seek::Current)
            count += 1
            bytesize += msg.bytesize
            requeued.insert(sp, msg.timestamp)
          rescue ex : IO::EOFError | IndexError
            @log.warn(exception: ex) { "Corrupt data at segment #{seg_id} pos #{pos}, skipping rest of segment" }
            break
          end
        end
        @requeued = requeued
        @size = count
        @bytesize = bytesize
        @empty.set empty?
        # We don't have to reset any pointer when we've read through all messages
        # since we're always using the requeued index.
        @log.debug { "rebuilding delayed index done" }
      end

      def first_delayed? : Envelope?
        raise ClosedError.new if @closed
        sp = @requeued.first? || return
        seg = @segments[sp.segment]
        begin
          msg = BytesMessage.from_bytes(seg.to_slice + sp.position)
          Envelope.new(sp, msg, redelivered: true, segment: seg)
        rescue ex
          raise MessageStore::Error.new(seg, cause: ex)
        end
      end

      def shift_delayed?(consumer = nil) : Envelope?
        raise ClosedError.new if @closed
        sp = @requeued.shift? || return
        segment = @segments[sp.segment]
        begin
          msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
          @bytesize -= sp.bytesize
          @size -= 1
          @empty.set true if @size.zero?
          Envelope.new(sp, msg, redelivered: true, segment: segment)
        rescue ex
          raise MessageStore::Error.new(segment, cause: ex)
        end
      end

      # Overload to add the segment position to our "index"
      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        was_empty = @size.zero?
        sp = write_to_disk(msg)
        requeued.insert(sp, msg.timestamp)
        @bytesize += sp.bytesize
        @size += 1
        @empty.set false if was_empty
        sp
      end

      def requeue(sp : SegmentPosition)
        raise "BUG: messages should never be requeued to DelayedMessageStore"
      end
    end
  end
end
