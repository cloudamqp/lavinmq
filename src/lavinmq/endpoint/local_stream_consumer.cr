require "./local_consumer"
require "../amqp/stream/stream_cursor"
require "../amqp/stream/filters/kv"
require "../amqp/stream/filters/x_stream_filter"
require "../amqp/stream/filters/gis"

module LavinMQ
  module Endpoint
    # A LocalConsumer of a Stream. Reads from its own offset, which it
    # tracks like an AMQP stream consumer (see AMQP::StreamConsumer): from
    # `x-stream-offset` if given, otherwise from the offset stored for its
    # consumer tag.
    class LocalStreamConsumer < LocalConsumer
      include AMQP::StreamCursor

      property offset : Int64
      property segment : UInt32
      property pos : UInt32
      property? segment_acquired = false
      property segment_since = RoughTime.instant
      getter requeued = Deque(SegmentPosition).new
      @filters : Array(AMQP::StreamFilter)
      @filter_match_all = true
      @match_unfiltered = false
      @track_offset = false
      @new_message_available = BoolChannel.new(false)

      def initialize(session : LocalSession, @stream : AMQP::Stream, tag : String, no_ack : Bool,
                     prefetch_count : UInt16, args : AMQ::Protocol::Table)
        # Like AMQP::StreamConsumer: a stream consumer's position moves with its acks
        raise Refused.new("406 - Stream consumers must acknowledge messages") if no_ack
        raise Refused.new("406 - Stream consumers must have a prefetch limit") if prefetch_count.zero?
        offset = args["x-stream-offset"]?
        case offset
        when Nil
          @track_offset = true
        when Int, Time, "first", "next", "last"
          case tracking = args["x-stream-automatic-offset-tracking"]?
          when Bool   then @track_offset = tracking
          when String then @track_offset = tracking == "true"
          end
        else
          raise Refused.new("406 - x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
        end
        @filters = AMQP::StreamFilter.from_arguments(args)
        @filter_match_all = args["x-filter-match-type"]?.try(&.to_s.downcase) != "any"
        @match_unfiltered = args["x-stream-match-unfiltered"]? == true
        @offset, @segment, @pos = @stream.find_offset(offset, tag, @track_offset)
        super(session, @stream, tag, no_ack: false, exclusive: false, prefetch_count: prefetch_count)
      end

      def cancel
        return if closed?
        super
        @new_message_available.close
      end

      def settled(sp : SegmentPosition, ack : Bool, requeue : Bool) : Nil
        decrement_unacked
        if ack
          begin
            @stream.store_consumer_offset(tag, @offset) if @track_offset
          rescue MessageStore::ClosedError
          end
        elsif requeue
          @requeued.push(sp)
          @new_message_available.set(true) if @requeued.size == 1
        end
      end

      def waiting_for_messages? : Bool
        (@offset + prefetch_count) >= @stream.last_offset && accepts?
      end

      def notify_new_message
        @new_message_available.set(true)
      end

      def filter_match?(msg_headers) : Bool
        return true if @filters.empty?
        if @match_unfiltered
          return true unless msg_headers.try &.has_key?("x-stream-filter-value")
        end
        return false unless headers = msg_headers
        if @filter_match_all
          @filters.all?(&.match?(headers))
        else
          @filters.any?(&.match?(headers))
        end
      end

      protected def get(& : Envelope -> Nil) : Bool
        @stream.consume_get(self) { |env| yield env }
      end

      protected def queue_ready? : Bool
        return true unless @requeued.empty?
        return true if @offset <= @stream.last_offset
        @new_message_available.set(false)
        # A publish between the check above and the reset would be missed
        @offset <= @stream.last_offset
      rescue ::Channel::ClosedError
        false
      end

      # Streams wake their consumers through notify_new_message rather than
      # the queue's empty channel. Only waiting for messages watches it: the
      # flag is cleared by queue_ready?, so it can be set while at capacity or
      # paused, and receiving it there would spin without yielding.
      private def wait_until_ready : Bool
        loop do
          return false if closed?
          if unacked >= prefetch_count
            wait(has_capacity.when_true)
            next
          end
          if @stream.state.paused?
            wait(@stream.paused.when_false)
            next
          end
          unless queue_ready?
            @new_message_available.when_true.receive?
            next
          end
          return true
        end
      end
    end
  end
end
