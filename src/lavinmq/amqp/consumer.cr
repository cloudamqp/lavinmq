require "log"
require "wait_group"
require "../amqp"
require "../logger"
require "./queue_consumer"

module LavinMQ
  module AMQP
    # A consumer on an AMQP channel: delivers Basic.Deliver frames to the
    # client, and shares the channel's global prefetch and flow
    class Consumer < QueueConsumer
      Log = LavinMQ::Log.for "amqp.consumer"
      getter channel
      @metadata : ::Log::Metadata

      def initialize(@channel : AMQP::Channel, queue : Queue, frame : AMQP::Frame::Basic::Consume)
        priority = consumer_priority(frame) # Must be before ConsumeOk, can close channel
        @metadata = @channel.@metadata.extend({consumer: frame.consumer_tag})
        super(queue, frame.consumer_tag, frame.no_ack, frame.exclusive, priority,
          @channel.prefetch_count, @channel.flow?, Logger.new(Log, @metadata))
      end

      private def global_capacity_signal : ::Channel(Nil)?
        ch = @channel
        ch.has_capacity.when_true unless ch.has_capacity?
      end

      private def global_prefetch_full? : Bool
        @channel.global_prefetch_count > 0 && @channel.unacked.size >= @channel.global_prefetch_count
      end

      private def count_delivery(redelivered : Bool) : Nil
        @channel.increment_deliver_count(redelivered, @no_ack)
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        track_unacked unless @no_ack || recover
        delivery_tag = @channel.next_delivery_tag(@queue, sp, @no_ack, self)
        deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
          delivery_tag,
          redelivered,
          msg.exchange_name, msg.routing_key)
        @channel.deliver(deliver, msg, redelivered, flush: false)
      end

      def cancel
        @channel.cancel_consumer(self)
      end

      private def consumer_priority(frame) : Int32
        case prio = frame.arguments["x-priority"]
        when Int then prio.to_i32
        else          raise LavinMQ::Error::PreconditionFailed.new("x-priority must be an integer")
        end
      rescue KeyError
        0
      rescue OverflowError
        raise LavinMQ::Error::PreconditionFailed.new("x-priority out of bounds, must fit a 32-bit integer")
      end

      def unacked_messages
        @channel.unacked
      end

      def details_tuple
        channel_details = @channel.details_tuple
        {
          queue: {
            name:  @queue.name,
            vhost: @queue.vhost.name,
          },
          consumer_tag:    @tag,
          exclusive:       @exclusive,
          ack_required:    !@no_ack,
          prefetch_count:  @prefetch_count,
          priority:        @priority,
          channel_details: {
            peer_host:       channel_details[:connection_details][:peer_host],
            peer_port:       channel_details[:connection_details][:peer_port],
            connection_name: channel_details[:connection_details][:name],
            user:            channel_details[:user],
            number:          channel_details[:number],
            name:            channel_details[:name],
          },
        }
      end

      def flush
        @channel.flush
        Fiber.yield # We are waiting so might as well let other fibers run to improve fairness
      end
    end
  end
end
