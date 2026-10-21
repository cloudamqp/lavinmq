require "../consumer"
require "./stream_consumer_arguments"

module LavinMQ
  module AMQP
    # An AMQP consumer of a Stream, reading from its own StreamCursor
    class StreamConsumer < Consumer
      getter cursor : StreamCursor
      @track_offset = false

      def initialize(channel : AMQP::Channel, queue : Stream, frame : AMQP::Frame::Basic::Consume)
        @cursor, @track_offset = StreamConsumerArguments.parse(queue, frame.consumer_tag, frame.arguments,
          frame.exclusive, frame.no_ack, channel.prefetch_count, channel.global_prefetch_count)
        super
        @new_message_available = BoolChannel.new(false)
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
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          stream_queue.consume_get(cursor) do |env|
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
      rescue ex : ClosedError | Queue::ClosedError | AMQP::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      ensure
        @deliver_loop_running.set(false, :release)
      end

      private def wait_for_queue_ready
        if @cursor.caught_up? # unlocked, a stale answer only delays or repeats a wait
          @log.debug { "Waiting for queue not to be empty" }
          flush
          select
          when @new_message_available.when_true.receive
            @log.debug { "Queue is not empty - new message notification received" }
            @new_message_available.set(false) # Reset the flag
          when @notify_closed.receive
          end
          true
        end
      end

      def notify_new_message
        ensure_deliver_loop
        @new_message_available.set(true)
      end

      private def stream_queue : Stream
        @queue.as(Stream)
      end

      def waiting_for_messages?
        (@cursor.offset + @prefetch_count) >= stream_queue.last_offset && accepts?
      end

      def ack(sp)
        begin
          stream_queue.store_consumer_offset(@tag, @cursor.offset) if @track_offset
        rescue MessageStore::ClosedError
          # The queue was closed/deleted while this ack was in flight. Storing the
          # offset is now a no-op; don't let it tear down the connection read_loop.
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
