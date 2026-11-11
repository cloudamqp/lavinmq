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

      # Data that can't be read is given up right away, unless the queue
      # pauses for it, see Queue#pauses_for_unreadable_data?
      @give_up_unreadable : Bool

      def initialize(*args, give_up_unreadable : Bool = true, **kwargs)
        @give_up_unreadable = give_up_unreadable
        super(*args, **kwargs)
        # A segment that failed to load closed the store and unmapped its
        # segments, so they can't be indexed
        unless @closed
          close_on_error { build_index }
          release_unreadable_unless_paused
        end
      end

      private def release_unreadable_unless_paused : Nil
        release_unreadable if @give_up_unreadable
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
          rescue ex : CorruptDataError
            # Left on disk, see MessageStore#skip_corrupt_tail
            @log.error(exception: ex) do
              "Message at segment #{seg_id} pos #{pos} can't be read, skipping the rest of the segment: " \
              "#{mfile.path} #{pos.try { |p| hexdump(mfile, p) }}"
            end
            mark_unreadable(seg_id)
            count_readable(seg_id, mfile, pos) if pos
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

      # Messages that can't be read are dropped, see #drop_requeued, and
      # given up right away, unless the queue pauses for them, see
      # #release_unreadable_unless_paused
      def first_delayed? : Envelope?
        raise ClosedError.new if @closed
        while sp = @requeued.first?
          if env = requeued_envelope?(sp)
            return env
          end
          @requeued.shift?
          drop_requeued(sp)
          release_unreadable_unless_paused
        end
      end

      def shift_delayed?(consumer = nil) : Envelope?
        raise ClosedError.new if @closed
        while sp = @requeued.shift?
          if env = requeued_envelope?(sp, shift: true)
            @bytesize -= sp.bytesize
            @size -= 1
            @empty.set true if @size.zero?
            return env
          end
          drop_requeued(sp)
          release_unreadable_unless_paused
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

      def first_due? : Bool
        requeued.first_due?(RoughTime.unix_ms)
      end

      # Puts a message that was shifted, but not delivered nor deleted, back in
      # the index, due in `delay_ms` from now
      def delay_again(sp : SegmentPosition, delay_ms : Int) : Nil
        raise ClosedError.new if @closed
        was_empty = @size.zero?
        # The index adds the message's own delay to the timestamp
        requeued.insert(sp, RoughTime.unix_ms + delay_ms - sp.delay)
        @bytesize += sp.bytesize
        @size += 1
        @empty.set false if was_empty
      end

      def purge_all
        while sp = @requeued.shift?
          @size -= 1
          @bytesize -= sp.bytesize
          delete(sp) if @segments.has_key?(sp.segment)
        end
        @empty.set true
        release_unreadable
      end

      def requeue(sp : SegmentPosition)
        raise "BUG: messages should never be requeued to DelayedMessageStore"
      end
    end
  end
end
