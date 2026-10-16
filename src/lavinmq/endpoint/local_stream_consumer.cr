require "./local_consumer"
require "../amqp/stream/stream_consumption"

module LavinMQ
  module Endpoint
    # A LocalConsumer of a Stream, reading from its own offset like an AMQP
    # stream consumer (see AMQP::StreamConsumption): from `x-stream-offset`
    # if given, otherwise from the offset stored for its consumer tag.
    class LocalStreamConsumer < LocalConsumer
      include AMQP::StreamConsumption

      def initialize(session : LocalSession, stream : AMQP::Stream, tag : String, no_ack : Bool,
                     exclusive : Bool, prefetch_count : UInt16, args : AMQ::Protocol::Table)
        setup_stream_consumer(stream, tag, args, exclusive, no_ack, prefetch_count, 0_u16)
        super(session, stream, tag, no_ack, exclusive, prefetch_count)
      end
    end
  end
end
