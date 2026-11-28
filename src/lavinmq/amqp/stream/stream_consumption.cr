require "../queue_consumer"
require "../../bool_channel"
require "../../config"
require "./filters/consumer_filter"
require "./stream_cursor"
require "./stream_offset"

module LavinMQ
  module AMQP
    # How a QueueConsumer consumes a Stream: through a cursor of its own,
    # whose offset it tracks with its acks, redelivering what it rejects
    # with requeue. Included by AMQP::StreamConsumer and
    # Endpoint::LocalStreamConsumer, whose initializers set `@cursor` from
    # #setup_stream_consumer before QueueConsumer's.
    module StreamConsumption
      getter cursor : StreamCursor
      @track_offset = false
      @new_message_available = BoolChannel.new(false)

      # Validates the consume and returns the cursor it starts reading from
      private def setup_stream_consumer(stream : Stream, tag : String, arguments : AMQP::Table,
                                        exclusive : Bool, no_ack : Bool, prefetch_count : UInt16,
                                        global_prefetch_count : UInt16) : StreamCursor
        if exclusive
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must not be exclusive")
        end
        if no_ack
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must acknowledge messages")
        end
        if prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers must have a prefetch limit")
        end
        unless global_prefetch_count.zero?
          raise LavinMQ::Error::PreconditionFailed.new("Stream consumers does not support global prefetch limit")
        end
        if arguments.has_key? "x-priority"
          raise LavinMQ::Error::PreconditionFailed.new("x-priority not supported on streams")
        end
        start = StreamOffset.from_amqp(arguments["x-stream-offset"]?)
        @track_offset = track_offset?(tag, arguments, start)
        filter = ConsumerFilter.from_arguments(arguments)
        # The stored offset wins when tracking offsets or when no offset is given
        stored_offset_tag = tag if @track_offset || start.nil?
        new_cursor(stream, start || StreamOffset::Absolute.new(0), filter, stored_offset_tag)
      end

      # A seek that reaches data that can't be read pauses or fails the stream,
      # see Stream#cursor. Only this consumer is refused for it.
      private def new_cursor(stream : Stream, start, filter, stored_offset_tag) : StreamCursor
        stream.cursor(start, filter, stored_offset_tag)
      rescue MessageStore::Error
        raise LavinMQ::Error::PreconditionFailed.new("Message data in stream '#{stream.name}' can't be read, see its state")
      end

      private def track_offset?(tag : String, arguments : AMQP::Table, start : StreamOffset::Any?) : Bool
        return !tag.starts_with?("amq.ctag-") if start.nil?
        case tracking = arguments["x-stream-automatic-offset-tracking"]?
        when Bool   then tracking
        when String then tracking == "true"
        else             false
        end
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        iterations = 0
        yield_each_delivered_bytes = Config.instance.yield_each_delivered_bytes

        loop do
          wait_for_capacity
          loop do
            raise QueueConsumer::ClosedError.new if @closed
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
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
      rescue ex : QueueConsumer::ClosedError | Queue::ClosedError | Channel::ClosedError | ::Channel::ClosedError
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
        super
      end
    end
  end
end
