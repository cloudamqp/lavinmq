require "../../../message_store"
require "./delayed_requeued_store"

module LavinMQ::AMQP
  class DelayedExchangeQueue < Queue
    class DelayedMessageStore < MessageStore
      @requeued : RequeuedStore = DelayedRequeuedStore.new

      private def requeued : DelayedRequeuedStore
        @requeued.as(DelayedRequeuedStore)
      end

      def time_to_next_expiration? : Time::Span?
        requeued.time_to_next_expiration?
      end

      def initialize(*args, **kwargs)
        super
        order_messages
      end

      def order_messages
        while env = shift?
          requeued.insert(env.segment_position, env.message.timestamp)
        end
      end

      def push(msg) : SegmentPosition
        was_empty = @size.zero?
        sp = super
        j
        # make sure that we don't read from disk, only from requeued
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
