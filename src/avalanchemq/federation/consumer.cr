require "../connection"

module AvalancheMQ
  class Upstream
    class Consumer < Connection
      def initialize(@upstream : QueueUpstream, @pub : Publisher, @federated_q : Queue)
        @log = @upstream.log.dup
        @log.progname += " consumer"
        super(@upstream.uri, @log)
        set_prefetch
      end

      def start
        return unless @federated_q.immediate_delivery?
        upstream_read_loop
      end

      private def upstream_read_loop
        consume
        loop do
          AMQP::Frame.decode(@socket, @buffer) do |frame|
            @log.debug { "Read #{frame.inspect}" }
            case frame
            when AMQP::Basic::Deliver
              @pub.send_basic_publish(frame, @federated_q.name)
            when AMQP::HeaderFrame
              @pub.write(frame)
            when AMQP::BodyFrame
              @pub.write(frame)
            when AMQP::Connection::CloseOk
              raise ClosedConnection.new
            when AMQP::Connection::Close
              write AMQP::Connection::CloseOk.new
              raise UnexpectedFrame.new(frame)
            when AMQP::Basic::Cancel
              write AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
            when AMQP::Channel::Close
              write AMQP::Channel::CloseOk.new(frame.channel)
            else
              raise UnexpectedFrame.new(frame)
            end
          end
        end
      rescue ClosedConnection
        nil
      ensure
        @socket.close
      end

      class ClosedConnection < Exception; end

      private def set_prefetch
        write AMQP::Basic::Qos.new(1_u16, 0_u32, @upstream.prefetch, false)
        AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Basic::QosOk) }
      end

      private def consume
        queue_name = @upstream.queue.not_nil!
        write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, true,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Queue::DeclareOk) }.as(AMQP::Queue::DeclareOk)
        queue = frame.queue_name
        no_ack = @upstream.ack_mode == AckMode::NoAck
        write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
          false, no_ack, false, false, {} of String => AMQP::Field)
        AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Basic::ConsumeOk) }
      end

      def ack(delivery_tag)
        write AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
      end

      def reject(delivery_tag)
        write AMQP::Basic::Reject.new(1_u16, delivery_tag.to_u64, false)
      end
    end
  end
end
