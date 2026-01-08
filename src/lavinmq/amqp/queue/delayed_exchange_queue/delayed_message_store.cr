require "../../../message_store"
require "./delayed_requeued_store"

module LavinMQ::AMQP
  class DelayedExchangeQueue < Queue
    # A delayed exchange queue must have its messages order by "expire at"
    # so the message expire loop will look at the right message. To acheive this
    # messages are always added to a custom requeued store. This requeued store
    # acts as a inmemory index where messages are ordered based on when they
    # should be published.
    # The reason why the requeued store is used, is that #shift and #first? will
    # look for any requeued messages first, then read the next from disk. For a
    # delayed exchange queue we never want to read messages in the order they
    # arrived (was written to disk).
    class DelayedMessageStore < MessageStore
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
        # Unfortunately we have to read all messages and build an "index"
        while env = shift?
          requeued.insert(env.segment_position, env.message.timestamp)
        end
        # We don't have to reset any pointer when we've read through all messages
        # since we're always using the requeued index.
      end

      # Overload to add the segment position to our "index"
      def push(msg) : SegmentPosition
        was_empty = @size.zero?
        sp = super

        # By always seeking to end of the last segment we prevent #shift and #first?
        # to read from file (if requeued for some reason is empty etc) because
        @rfile_id = @wfile_id
        @rfile = @wfile
        @rfile.seek(0, IO::Seek::End)
        requeued.insert(sp, msg.timestamp)

        @empty.set false if was_empty
        sp
      end

      def requeue(sp : SegmentPosition)
        raise "BUG: messages should never be requeued to DelayedMessageStore"
      end
    end
  end
end
