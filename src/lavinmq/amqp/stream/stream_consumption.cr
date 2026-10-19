require "../queue_consumer"
require "./stream_cursor"
require "../../segment_position"
require "../../rough_time"
require "../../bool_channel"
require "./filters/kv"
require "./filters/x_stream_filter"
require "./filters/gis"

module LavinMQ
  module AMQP
    # How a QueueConsumer consumes a Stream: from its own offset, which it
    # tracks with its acks, delivering only messages that match its filters,
    # and redelivering what it rejects with requeue. Included by
    # AMQP::StreamConsumer and Endpoint::LocalStreamConsumer.
    #
    # An includer calls #setup_stream_consumer from its initializer.
    module StreamConsumption
      include StreamCursor

      @filters = Array(StreamFilter).new
      @filter_match_all = true
      @match_unfiltered = false
      @track_offset = false
      @new_message_available = BoolChannel.new(false)

      # Validates the consume and finds where it starts reading
      private def setup_stream_consumer(stream : Stream, tag : String, arguments : AMQP::Table,
                                        exclusive : Bool, no_ack : Bool, prefetch_count : UInt16,
                                        global_prefetch_count : UInt16) : Nil
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
        validate_stream_offset(tag, arguments)
        @filters = StreamFilter.from_arguments(arguments)
        validate_filter_match_type(arguments)
        case match_unfiltered = arguments["x-stream-match-unfiltered"]?
        when Bool
          @match_unfiltered = match_unfiltered
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-match-unfiltered must be a boolean")
        end
        @offset, @segment, @pos = stream.find_offset(arguments["x-stream-offset"]?, tag, @track_offset)
      end

      private def validate_stream_offset(tag, arguments)
        case arguments["x-stream-offset"]?
        when Nil
          @track_offset = true unless tag.starts_with?("amq.ctag-")
        when Int, Time, "first", "next", "last"
          case tracking = arguments["x-stream-automatic-offset-tracking"]?
          when Bool
            @track_offset = tracking
          when String
            @track_offset = tracking == "true"
          end
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
        end
      end

      private def validate_filter_match_type(arguments)
        case filter_match_type = arguments["x-filter-match-type"]?
        when String
          if filter_match_type.downcase == "all"
            @filter_match_all = true
          elsif filter_match_type.downcase == "any"
            @filter_match_all = false
          else
            raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
          end
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-filter-match-type must be 'any' or 'all'")
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
          stream_queue.consume_get(self) do |env|
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
        # @requeued is pushed to under the queue's msg store lock, which is not
        # taken here; a push this misses also sets @new_message_available
        if @offset > stream_queue.last_offset && @requeued.empty?
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
        (@offset + @prefetch_count) >= stream_queue.last_offset && accepts?
      end

      def ack(sp)
        begin
          stream_queue.store_consumer_offset(@tag, @offset) if @track_offset
        rescue MessageStore::ClosedError
          # The queue was closed/deleted while this ack was in flight. Storing the
          # offset is now a no-op; don't let it tear down the connection read_loop.
        end
        super
      end

      def reject(sp, requeue : Bool)
        super
        if requeue
          # Set after the push, so a deliver loop that found @requeued empty
          # is woken up by the notification instead
          was_empty = stream_queue.push_requeued(self, sp)
          @new_message_available.set(true) if was_empty
        end
      end

      def close
        return if closed?
        @new_message_available.close
        super
      end

      def filter_match?(msg_headers) : Bool
        return true if @filters.empty? # No consumer filters, always match
        if @match_unfiltered
          return true unless msg_headers.try &.has_key?("x-stream-filter-value")
        end
        return false unless headers = msg_headers

        case @filter_match_all
        when false
          @filters.any?(&.match?(headers))
        else
          @filters.all?(&.match?(headers))
        end
      end
    end
  end
end
