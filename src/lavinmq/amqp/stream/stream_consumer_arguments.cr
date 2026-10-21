require "../../error"
require "./filters/consumer_filter"
require "./stream_cursor"
require "./stream_offset"

module LavinMQ::AMQP
  # The arguments of a consume from a Stream, shared by AMQP::StreamConsumer
  # and Endpoint::LocalStreamConsumer
  module StreamConsumerArguments
    # Validates the consume and returns a cursor where it starts reading, and
    # whether its acks store the offset for `tag`
    def self.parse(stream : Stream, tag : String, arguments : AMQP::Table, exclusive : Bool,
                   no_ack : Bool, prefetch_count : UInt16,
                   global_prefetch_count : UInt16) : Tuple(StreamCursor, Bool)
      validate(arguments, exclusive, no_ack, prefetch_count, global_prefetch_count)
      start = StreamOffset.from_amqp(arguments["x-stream-offset"]?)
      track_offset = track_offset?(tag, arguments, start)
      filter = ConsumerFilter.from_arguments(arguments)
      start = resolve_start(stream, tag, start, track_offset)
      {stream.cursor(start, filter), track_offset}
    end

    private def self.validate(arguments, exclusive, no_ack, prefetch_count, global_prefetch_count)
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
    end

    private def self.track_offset?(tag, arguments, start : StreamOffset::Any?) : Bool
      return !tag.starts_with?("amq.ctag-") if start.nil?
      case tracking = arguments["x-stream-automatic-offset-tracking"]?
      when Bool   then tracking
      when String then tracking == "true"
      else             false
      end
    end

    # The stored offset wins when tracking offsets or when no offset is given
    private def self.resolve_start(stream, tag, start : StreamOffset::Any?, track_offset) : StreamOffset::Any
      if track_offset || start.nil?
        stored = stream.stored_offset(tag)
        return StreamOffset::Absolute.new(stored) if stored
      end
      start || StreamOffset::Absolute.new(0)
    end
  end
end
