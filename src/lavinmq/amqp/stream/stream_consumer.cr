require "../consumer"
require "./stream_consumption"

module LavinMQ
  module AMQP
    # A stream consumer on an AMQP channel, see StreamConsumption
    class StreamConsumer < Consumer
      include StreamConsumption

      def initialize(channel : AMQP::Channel, queue : Stream, frame : AMQP::Frame::Basic::Consume)
        @cursor = setup_stream_consumer(queue, frame.consumer_tag, frame.arguments, frame.exclusive,
          frame.no_ack, channel.prefetch_count, channel.global_prefetch_count)
        super
      end
    end
  end
end
