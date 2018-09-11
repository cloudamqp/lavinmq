require "../connection"

module AvalancheMQ
  class Shovel
    class Consumer < Connection
      def initialize(@source : Source, @in : Channel::Buffered(AMQP::Frame),
                     @out : Channel::Buffered(AMQP::Frame), @ack_mode : AckMode, log : Logger)
        @log = log.dup
        @log.progname += " consumer"
        @message_counter = 0_u32
        @message_count = 0_u32
        super(@source.uri, @log)
        set_prefetch
      end

      def on_done(&blk)
        @on_done = blk
      end

      def start
        spawn(channel_read_loop, name: "Shovel publisher #{@source.uri.host}#channel_read_loop")
        consume_loop
      end

      private def consume_loop
        consume
        loop do
          Fiber.yield if @out.full?
          frame = AMQP::Frame.decode(@socket)
          #@log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::HeaderFrame
            @out.send(frame)
          when AMQP::Basic::Deliver
            @out.send(frame)
          when AMQP::BodyFrame
            @out.send(frame)
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            raise UnexpectedFrame.new(frame) unless @out.closed?
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            break
          else
            raise UnexpectedFrame.new(frame)
          end
        rescue Channel::ClosedError
          @log.debug { "#consume_loop out channel closed" }
          break
        end
      rescue ex : Errno | IO::Error
        @log.info { ex.inspect }
      rescue ex
        @log.debug { ex.inspect }
      ensure
        @socket.close
      end

      private def channel_read_loop
        loop do
          frame = @in.receive
          case frame
          when AMQP::Basic::Ack
            @socket.write frame.to_slice unless @ack_mode == AckMode::NoAck
            @message_counter += 1
            if @source.delete_after == DeleteAfter::QueueLength &&
               @message_count <= @message_counter
              @socket.write AMQP::Connection::Close.new(200_u16,
                "Shovel done", 0_u16, 0_u16).to_slice
              @on_done.try &.call
            end
          when AMQP::Basic::Nack
            @socket.write frame.to_slice
          when AMQP::Basic::Return
            @socket.write frame.to_slice
          else
            @log.warn { "Unexpected frame #{frame}" }
          end
        end
      rescue ex : Channel::ClosedError
        @log.debug { "#channel_read_loop closed" }
        close("Shovel stopped")
      end

      private def set_prefetch
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
      end

      private def consume
        queue_name = @source.queue || ""
        passive = !queue_name.empty?
        @socket.write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, passive,
          false, true, true, false,
          {} of String => AMQP::Field).to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Queue::DeclareOk)
        if @source.exchange
          @socket.write AMQP::Queue::Bind.new(1_u16, 0_u16, frame.queue_name,
            @source.exchange.not_nil!,
            @source.exchange_key || "",
            true,
            {} of String => AMQP::Field).to_slice
        end
        queue = frame.queue_name
        @message_count = frame.message_count
        no_ack = @ack_mode == AckMode::NoAck
        consume = AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, no_ack, false, false,
          {} of String => AMQP::Field)
        @socket.write consume.to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Basic::ConsumeOk)
      end
    end
  end
end
