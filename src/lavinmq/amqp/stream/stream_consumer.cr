require "../consumer"
require "../../segment_position"
require "./filters/kv"
require "./filters/x_stream_filter"

module LavinMQ
  module AMQP
    class StreamConsumer < Consumer
      include SortableJSON
      property offset : Int64
      property segment : UInt32
      property pos : UInt32
      getter requeued = Deque(SegmentPosition).new
      @filters = Array(StreamFilter).new
      @filter_match_all = true
      @match_unfiltered = false
      @track_offset = false

      def initialize(@channel : Client::Channel, @queue : Stream, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        validate_preconditions(frame)
        offset = frame.arguments["x-stream-offset"]?
        @offset, @segment, @pos = stream_queue.find_offset(offset, @tag, @track_offset)
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
        validate_stream_offset(frame)
        validate_stream_filter(frame.arguments["x-stream-filter"]?)
        validate_filter_match_type(frame)
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
          arg.split(',').each do |f|
            @filters << XStreamFilter.new(f.strip)
          end
        when AMQ::Protocol::Table
          arg.each do |k, v|
            if k.to_s == "x-stream-filter"
              v.to_s.split(',').each do |f|
                @filters << XStreamFilter.new(f.strip)
              end
            else
              @filters << KVFilter.new(k.to_s, v.to_s)
            end
          end
        when Array
          arg.each do |f|
            validate_stream_filter(f)
          end
        when Nil
          # noop
        else raise LavinMQ::Error::PreconditionFailed.new("x-stream-filter must be a string, table, or array")
        end
      end

      private def validate_filter_match_type(frame)
        case filter_match_type = frame.arguments["x-filter-match-type"]?
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
          flush
          select
          when @new_message_available.when_true.receive
            @log.debug { "Queue is not empty - new message notification received" }
            @new_message_available.set(false) # Reset the flag
          when @notify_closed.receive
          end
          return true
        end
      end

      def notify_new_message
        @new_message_available.set(true)
      end

      private def stream_queue : Stream
        @queue.as(Stream)
      end

      def waiting_for_messages?
        (@offset + @prefetch_count) >= stream_queue.last_offset && accepts?
      end

      def ack(sp)
        stream_queue.store_consumer_offset(@tag, @offset) if @track_offset
        super
      end

      def reject(sp, requeue : Bool)
        super
        if requeue
          @requeued.push(sp)
          @new_message_available.set(true) if @requeued.size == 1
        end
      end

      def close
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
