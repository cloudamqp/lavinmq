require "../connection"

module AvalancheMQ
  class Upstream
    class Consumer < Connection
      def initialize(@upstream : QueueUpstream)
        @log = @upstream.log.dup
        @log.progname += " consumer"
        super(@upstream.uri, @log)
      end

      @on_frame : Proc(AMQP::Frame, Nil)?

      def on_frame(&blk : AMQP::Frame -> Nil)
        @on_frame = blk
      end

      def run
        set_prefetch
        consume
        spawn(read_loop, name: "Upstream consumer #{@upstream.uri.host}#read_loop")
      end

      private def read_loop
        loop do
          AMQP::Frame.decode(@socket) do |frame|
            @log.debug { "Read socket #{frame.inspect}" }
            case frame
            when AMQP::Basic::Deliver, AMQP::HeaderFrame, AMQP::BodyFrame
              @on_frame.try &.call(frame)
              true
            when AMQP::Basic::Cancel
              unless frame.no_wait
                write AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
              end
              write AMQP::Connection::Close.new(320_u16, "Consumer cancelled", 0_u16, 0_u16)
              true
            when AMQP::Connection::Close
              write AMQP::Connection::CloseOk.new
              false
            when AMQP::Connection::CloseOk
              false
            else
              raise UnexpectedFrame.new(frame)
            end
          end || break
        end
      rescue ex : IO::Error | Errno | AMQP::FrameDecodeError
        @log.info "Consumer closed due to: #{ex.inspect}"
      ensure
        @log.debug "Closing socket"
        @socket.close
      end

      def forward(frame)
        @log.debug { "Read internal #{frame.inspect}" }
        case frame
        when AMQP::Basic::Ack
          unless @upstream.ack_mode == AckMode::NoAck
            write frame
          end
        when AMQP::Basic::Reject
          write frame
        else
          @log.warn { "Unexpected frame: #{frame.inspect}" }
        end
      end

      private def set_prefetch
        write AMQP::Basic::Qos.new(1_u16, 0_u32, @upstream.prefetch, false)
        AMQP::Frame.decode(@socket) { |f| f.as(AMQP::Basic::QosOk) }
      end

      private def consume
        queue_name = @upstream.queue.not_nil!
        write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, true,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.decode(@socket) { |f| f.as(AMQP::Queue::DeclareOk) }
        queue = frame.queue_name
        no_ack = @upstream.ack_mode == AckMode::NoAck
        write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
          false, no_ack, false, false, {} of String => AMQP::Field)
        AMQP::Frame.decode(@socket) { |f| f.as(AMQP::Basic::ConsumeOk) }
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
