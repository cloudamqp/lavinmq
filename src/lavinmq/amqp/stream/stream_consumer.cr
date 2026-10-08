require "../consumer"
require "../../segment_position"
require "../../rough_time"
require "./filters/consumer_filter"
require "./stream_cursor"
require "./stream_cursor_consumer"
require "./stream_offset"

module LavinMQ
  module AMQP
    class StreamConsumer < Consumer
      include SortableJSON
      include StreamCursorConsumer
      getter cursor : StreamCursor
      @track_offset = false

      def initialize(@channel : Client::Channel, @queue : Stream, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        validate_preconditions(frame)
        start = StreamOffset.from_amqp(frame.arguments["x-stream-offset"]?)
        @track_offset = track_offset?(frame, start)
        filter = ConsumerFilter.from_arguments(frame.arguments)
        @cursor = stream_queue.cursor(resolve_start(start), filter)
        super
        @new_message_available = BoolChannel.new(false)
      end

      private def validate_preconditions(frame)
        if frame.exclusive
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must not be exclusive")
        end
        if frame.no_ack
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must acknowledge messages")
        end
        if @channel.prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must have a prefetch limit")
        end
        unless @channel.global_prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers does not support global prefetch limit")
        end
        if frame.arguments.has_key? "x-priority"
          raise LavinMQ::Error::PreconditionFailed.new("x-priority not supported on streams")
        end
      end

      private def track_offset?(frame, start : StreamOffset::Any?) : Bool
        return !@tag.starts_with?("amq.ctag-") if start.nil?
        case tracking = frame.arguments["x-stream-automatic-offset-tracking"]?
        when Bool   then tracking
        when String then tracking == "true"
        else             false
        end
      end

      # The stored offset wins when tracking offsets or when no offset is given
      private def resolve_start(start : StreamOffset::Any?) : StreamOffset::Any
        if @track_offset || start.nil?
          stored = stream_queue.stored_offset(@tag)
          return StreamOffset::Absolute.new(stored) if stored
        end
        start || StreamOffset::Absolute.new(0)
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
          stream_queue.consume_get(self.cursor) do |env|
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
          stream_queue.requeue(@cursor, sp)
          @new_message_available.set(true)
        end
      end

      def close
        return if closed?
        @new_message_available.close
        super
      end
    end
  end
end
