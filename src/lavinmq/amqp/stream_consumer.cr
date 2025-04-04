require "./consumer"
require "../segment_position"

module LavinMQ
  module AMQP
    class StreamConsumer < Consumer
      include SortableJSON
      property offset : Int64
      property segment : UInt32
      property pos : UInt32
      getter requeued = Deque(SegmentPosition).new
      @filter = Array(String).new
      @match_unfiltered = false
      @track_offset = false

      def initialize(@channel : Client::Channel, @queue : StreamQueue, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        validate_preconditions(frame)
        offset = frame.arguments["x-stream-offset"]?
        @offset, @segment, @pos = stream_queue.find_offset(offset, @tag, @track_offset)
        super
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
          raise LavinMQ::Error::PreconditionFailed.new("x-priority not supported on stream queues")
        end
        validate_stream_offset(frame)
        validate_stream_filter(frame.arguments["x-stream-filter"]?)
        case match_unfiltered = frame.arguments["x-stream-match-unfiltered"]?
        when Bool
          @match_unfiltered = match_unfiltered
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-match-unfiltered must be a boolean")
        end
      end

      private def validate_stream_offset(frame)
        case frame.arguments["x-stream-offset"]?
        when Nil
          @track_offset = true unless @tag.starts_with?("amq.ctag-")
        when Int, Time, "first", "next", "last"
          case frame.arguments["x-stream-automatic-offset-tracking"]?
          when Bool
            @track_offset = frame.arguments["x-stream-automatic-offset-tracking"]?.as(Bool)
          when String
            @track_offset = frame.arguments["x-stream-automatic-offset-tracking"]? == "true"
          end
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
        end
      end

      private def validate_stream_filter(arg)
        case arg
        when String
          @filter = arg.split(',').sort!
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-filter-value must be a string")
        end
      end

      private def deliver_loop
        i = 0
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
          stream_queue.consume_get(self) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue ex : ClosedError | Queue::ClosedError | AMQP::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      end

      private def wait_for_queue_ready
        if @offset > stream_queue.last_offset && @requeued.empty?
          @log.debug { "Waiting for queue not to be empty" }
          select
          when stream_queue.new_messages.receive
            @log.debug { "Queue is not empty" }
          when @has_requeued.receive
            @log.debug { "Got a requeued message" }
          when @notify_closed.receive
          end
          return true
        end
      end

      @has_requeued = ::Channel(Nil).new

      private def stream_queue : StreamQueue
        @queue.as(StreamQueue)
      end

      def ack(sp)
        stream_queue.store_consumer_offset(@tag, @offset) if @track_offset
        super
      end

      def reject(sp, requeue : Bool)
        super
        if requeue
          @requeued.push(sp)
          @has_requeued.try_send? nil if @requeued.size == 1
        end
      end

      def filter_match?(msg_headers) : Bool
        return true if @filter.empty?
        if msg_filters = filter_value_from_msg_headers(msg_headers)
          @filter.each do |consumer_filter|
            return false unless msg_filters.find { |f| f == consumer_filter }
          end
          true
        else
          @match_unfiltered
        end
      end

      private def filter_value_from_msg_headers(msg_headers) : Array(String)?
        msg_headers.try &.fetch("x-stream-filter-value", nil).try &.to_s.split(',')
      end
    end
  end
end
