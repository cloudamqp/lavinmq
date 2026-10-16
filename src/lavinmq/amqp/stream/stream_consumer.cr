require "../consumer"
require "./stream_consumption"

module LavinMQ
  module AMQP
    # An AMQP consumer of a Stream, see StreamConsumption
    class StreamConsumer < Consumer
      include StreamConsumption

      def initialize(channel : AMQP::Channel, queue : Stream, frame : AMQP::Frame::Basic::Consume)
        setup_stream_consumer(queue, frame.consumer_tag, frame.arguments, frame.exclusive,
          frame.no_ack, channel.prefetch_count, channel.global_prefetch_count)
        super
      end
    end
  end
end
