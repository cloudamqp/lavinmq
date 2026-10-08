require "./local_consumer"
require "../amqp/stream/stream_cursor"
require "../amqp/stream/stream_cursor_consumer"
require "../amqp/stream/stream_offset"
require "../amqp/stream/filters/consumer_filter"

module LavinMQ
  module Endpoint
    # A LocalConsumer of a Stream. Reads through its own StreamCursor, which
    # starts like an AMQP stream consumer's (see AMQP::StreamConsumer): from
    # the offset stored for its consumer tag when tracking offsets or when no
    # `x-stream-offset` is given, otherwise from `x-stream-offset`.
    class LocalStreamConsumer < LocalConsumer
      include AMQP::StreamCursorConsumer

      getter cursor : AMQP::StreamCursor
      @track_offset = false
      @new_message_available = BoolChannel.new(false)

      def initialize(session : LocalSession, @stream : AMQP::Stream, tag : String, no_ack : Bool,
                     prefetch_count : UInt16, args : AMQ::Protocol::Table)
        # Like AMQP::StreamConsumer: a stream consumer's position moves with its acks
        raise Refused.new("406 - Stream consumers must acknowledge messages") if no_ack
        raise Refused.new("406 - Stream consumers must have a prefetch limit") if prefetch_count.zero?
        start = begin
          AMQP::StreamOffset.from_amqp(args["x-stream-offset"]?)
        rescue ex : LavinMQ::Error::PreconditionFailed
          raise Refused.new("406 - #{ex.message}")
        end
        @track_offset = track_offset?(tag, args, start)
        @cursor = @stream.cursor(resolve_start(tag, start), AMQP::ConsumerFilter.from_arguments(args))
        super(session, @stream, tag, no_ack: false, exclusive: false, prefetch_count: prefetch_count)
      end

      private def track_offset?(tag, args, start) : Bool
        return !tag.starts_with?("amq.ctag-") if start.nil?
        case tracking = args["x-stream-automatic-offset-tracking"]?
        when Bool   then tracking
        when String then tracking == "true"
        else             false
        end
      end

      # The stored offset wins when tracking offsets or when no offset is given
      private def resolve_start(tag, start) : AMQP::StreamOffset::Any
        if @track_offset || start.nil?
          stored = @stream.stored_offset(tag)
          return AMQP::StreamOffset::Absolute.new(stored) if stored
        end
        start || AMQP::StreamOffset::Absolute.new(0)
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
            @stream.store_consumer_offset(tag, @cursor.offset) if @track_offset
          rescue MessageStore::ClosedError
          end
        elsif requeue
          @stream.requeue(@cursor, sp)
          @new_message_available.set(true)
        end
      rescue ::Channel::ClosedError
      end

      def waiting_for_messages? : Bool
        (@cursor.offset + prefetch_count) >= @stream.last_offset && accepts?
      end

      def notify_new_message
        @new_message_available.set(true)
      rescue ::Channel::ClosedError
      end

      protected def get(& : Envelope -> Nil) : Bool
        @stream.consume_get(@cursor) { |env| yield env }
      end

      protected def queue_ready? : Bool
        return true unless @cursor.caught_up?
        @new_message_available.set(false)
        # A publish between the check above and the reset would be missed
        !@cursor.caught_up?
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
