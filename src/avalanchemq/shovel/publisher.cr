require "../connection"

module AvalancheMQ
  class Shovel
    class Publisher < Connection
      def initialize(@destination : Destination, @in : Channel::Buffered(AMQP::Frame),
                     @out : Channel::Buffered(AMQP::Frame), @ack_mode : AckMode, log : Logger)
        @log = log.dup
        @log.progname += " publisher"
        @message_count = 0_u64
        @delivery_tags = Hash(UInt64, UInt64).new
        super(@destination.uri, @log)
        set_confirm if @ack_mode == AckMode::OnConfirm
      end

      def start
        channel_read_loop
        amqp_read_loop
      end

      private def send_basic_publish(frame)
        exchange = @destination.exchange.not_nil!
        routing_key = @destination.exchange_key || frame.routing_key
        mandatory = @ack_mode == AckMode::OnConfirm
        @socket.write AMQP::Basic::Publish.new(frame.channel, 0_u16, exchange, routing_key,
          mandatory, false).to_slice
        if mandatory
          @message_count += 1
          @delivery_tags[@message_count] = frame.delivery_tag
        else
          ack(frame.delivery_tag)
        end
      end

      private def amqp_read_loop
        loop do
          Fiber.yield if @out.full?
          frame = AMQP::Frame.decode(@socket)
          @log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::Basic::Nack
            next unless @ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| reject(t) }
            else
              reject(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Basic::Return
            reject(@message_count) if @ack_mode == AckMode::OnConfirm
          when AMQP::Basic::Ack
            next unless @ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| ack(t) }
            else
              ack(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            break
          else
            @log.warn { "Unexpected frame #{frame}" }
          end
        rescue Channel::ClosedError
          @log.debug { "#amqp_read_loop out channel closed" }
        rescue e : Errno | IO::Error
          @log.info { e.inspect }
        end
      ensure
        @socket.close
      end

      private def channel_read_loop
        spawn(name: "Shovel publisher #{@destination.uri.host}#channel_read_loop") do
          loop do
            frame = @in.receive
            case frame
            when AMQP::Basic::Deliver
              send_basic_publish(frame)
            when AMQP::HeaderFrame
              @socket.write frame.to_slice
            when AMQP::BodyFrame
              @socket.write frame.to_slice
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue Channel::ClosedError
            @log.debug { "#channel_read_loop closed" }
            close("Shovel stopped")
            break
          rescue e : Errno | IO::Error
            @log.debug { "#channel_read_loop #{e.inspect_with_backtrace}" }
            break
          end
        end
      ensure
        @socket.close
      end

      private def set_confirm
        @socket.write AMQP::Confirm::Select.new(1_u16, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Confirm::SelectOk)
      end

      private def with_multiple(delivery_tag)
        @delivery_tags.delete_if do |m, t|
          next false unless m <= delivery_tag
          yield t
          true
        end
      end

      private def ack(delivery_tag)
        @out.send AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
      end

      private def reject(delivery_tag)
        @out.send AMQP::Basic::Reject.new(1_u16, delivery_tag, false)
      end
    end
  end
end
