require "./local_consumer"
require "../amqp/stream/stream_consumer_arguments"

module LavinMQ
  module Endpoint
    # A LocalConsumer of a Stream, reading from its own StreamCursor like an
    # AMQP::StreamConsumer: from `x-stream-offset` if given, otherwise from
    # the offset stored for its consumer tag.
    class LocalStreamConsumer < LocalConsumer
      getter cursor : AMQP::StreamCursor
      @track_offset = false
      @new_message_available = BoolChannel.new(false)

      def initialize(session : LocalSession, stream : AMQP::Stream, tag : String, no_ack : Bool,
                     exclusive : Bool, prefetch_count : UInt16, args : AMQ::Protocol::Table)
        @cursor, @track_offset = AMQP::StreamConsumerArguments.parse(stream, tag, args, exclusive,
          no_ack, prefetch_count, 0_u16)
        super(session, stream, tag, no_ack, exclusive, prefetch_count)
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        iterations = 0
        yield_each_delivered_bytes = Config.instance.yield_each_delivered_bytes

        loop do
          wait_for_capacity
          loop do
            raise ClosedError.new if @closed
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          stream_queue.consume_get(@cursor) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
            delivered_bytes &+= env.segment_position.bytesize
          end
          iterations &+= 1
          if delivered_bytes >= yield_each_delivered_bytes || iterations >= 32_768
            delivered_bytes = 0
            iterations = 0
            Fiber.yield
          end
        end
      rescue ex : ClosedError | AMQP::Queue::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      ensure
        @deliver_loop_running.set(false, :release)
      end

      private def wait_for_queue_ready
        if @cursor.caught_up? # unlocked, a stale answer only delays or repeats a wait
          flush
          select
          when @new_message_available.when_true.receive
            @new_message_available.set(false)
          when @notify_closed.receive
          end
          true
        end
      end

      def notify_new_message
        @new_message_available.set(true)
      end

      private def stream_queue : AMQP::Stream
        @queue.as(AMQP::Stream)
      end

      def waiting_for_messages?
        (@cursor.offset + @prefetch_count) >= stream_queue.last_offset && accepts?
      end

      def ack(sp)
        begin
          stream_queue.store_consumer_offset(@tag, @cursor.offset) if @track_offset
        rescue MessageStore::ClosedError
          # The queue was closed or deleted while this ack was in flight
        end
        super
      end

      def reject(sp, requeue : Bool)
        super
        if requeue
          # Set after the push, so a deliver loop that found the cursor's
          # requeued messages empty is woken up by the notification instead
          was_empty = stream_queue.requeue(@cursor, sp)
          @new_message_available.set(true) if was_empty
        end
      end

      def close
        return if closed?
        @new_message_available.close
        stream_queue.close_cursor(@cursor)
        super
      end
    end
  end
end
