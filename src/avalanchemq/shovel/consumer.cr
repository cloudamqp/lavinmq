require "../connection"

module AvalancheMQ
  class Shovel
    class Consumer < Connection
      def initialize(@source : Source, @in : Channel(AMQP::Frame?),
                     @out : Channel(AMQP::Frame?), @ack_mode : AckMode, log : Logger)
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
        spawn(channel_read_loop, name: "Shovel consumer #{@source.uri.host}#channel_read_loop")
        consume_loop
      end

      private def consume_loop
        consume
        loop do
          Fiber.yield if @out.full?
          AMQP::Frame.decode(@socket, @buffer) do |frame|
            @log.debug { "Read socket #{frame.inspect}" }
            case frame
            when AMQP::HeaderFrame
              @out.send(frame)
              true
            when AMQP::Basic::Deliver
              @out.send(frame)
              true
            when AMQP::BodyFrame
              @out.send(frame)
              after_publish if @ack_mode == AckMode::NoAck
              true
            when AMQP::Connection::CloseOk
              false
            when AMQP::Connection::Close
              raise UnexpectedFrame.new(frame) unless @out.closed?
              write AMQP::Connection::CloseOk.new
              false
            else
              raise UnexpectedFrame.new(frame)
            end
          end || break
        end
      ensure
        @log.debug "Closing socket"
        @socket.close
      end

      private def channel_read_loop
        loop do
          frame = @in.receive
          @log.debug { "Read internal #{frame.inspect}" }
          case frame
          when AMQP::Basic::Ack
            unless @ack_mode == AckMode::NoAck
              write frame
            end
            after_publish
          when AMQP::Basic::Reject
            write frame
          when nil
            break
          else
            @log.warn { "Unexpected frame: #{frame.inspect}" }
          end
        end
      rescue ex
        @log.debug { "#channel_read_loop closed: #{Fiber.current.hash} #{ex.inspect_with_backtrace}" }
      ensure
        close("Shovel stopped")
      end

      private def after_publish
        @message_counter += 1
        if @source.delete_after == DeleteAfter::QueueLength && @message_count <= @message_counter
          write AMQP::Connection::Close.new(200_u16, "Shovel done", 0_u16, 0_u16)
          @on_done.try &.call
        end
      end

      private def set_prefetch
        write AMQP::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false)
        AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Basic::QosOk) }
      end

      private def consume
        queue_name = @source.queue || ""
        passive = !queue_name.empty?
        write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, passive,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Queue::DeclareOk) }
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Queue::DeclareOk)
        queue = frame.queue_name
        @message_count = frame.message_count
        if @source.exchange
          write AMQP::Queue::Bind.new(1_u16, 0_u16, frame.queue_name,
            @source.exchange.not_nil!,
            @source.exchange_key || "",
            false,
            {} of String => AMQP::Field)
          frame = AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Queue::DeclareOk) }
          raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Queue::BindOk)
        end
        no_ack = @ack_mode == AckMode::NoAck
        write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, no_ack, false, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Basic::ConsumeOk) }
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Basic::ConsumeOk)
      end
    end
  end
end
