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
          AMQP::Frame.from_io(@socket) do |frame|
            @log.debug { "Read socket #{frame.inspect}" }
            case frame
            when AMQP::Frame::Basic::Deliver, AMQP::Frame::Header, AMQP::Frame::Body
              @on_frame.try &.call(frame)
              true
            when AMQP::Frame::Basic::Cancel
              unless frame.no_wait
                write AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
              end
              write AMQP::Frame::Connection::Close.new(320_u16, "Consumer cancelled", 0_u16, 0_u16)
              true
            when AMQP::Frame::Connection::Close
              write AMQP::Frame::Connection::CloseOk.new
              false
            when AMQP::Frame::Connection::CloseOk
              false
            else
              raise UnexpectedFrame.new(frame)
            end
          end || break
        end
      rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
        @log.info "Consumer closed due to: #{ex.inspect}"
      ensure
        @log.debug "Closing socket"
        @socket.close
      end

      def forward(frame)
        @log.debug { "Read internal #{frame.inspect}" }
        case frame
        when AMQP::Frame::Basic::Ack
          unless @upstream.ack_mode == AckMode::NoAck
            write frame
          end
        when AMQP::Frame::Basic::Reject
          write frame
        else
          @log.warn { "Unexpected frame: #{frame.inspect}" }
        end
      end

      private def set_prefetch
        write AMQP::Frame::Basic::Qos.new(1_u16, 0_u32, @upstream.prefetch, false)
        AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::QosOk) }
      end

      private def consume
        queue_name = @upstream.queue.not_nil!
        write AMQP::Frame::Queue::Declare.new(1_u16, 0_u16, queue_name, true,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Queue::DeclareOk) }
        queue = frame.queue_name
        no_ack = @upstream.ack_mode == AckMode::NoAck
        write AMQP::Frame::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
          false, no_ack, false, false, {} of String => AMQP::Field)
        AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::ConsumeOk) }
      end

      def ack(delivery_tag)
        write AMQP::Frame::Basic::Ack.new(1_u16, delivery_tag, false)
      end

      def reject(delivery_tag)
        write AMQP::Frame::Basic::Reject.new(1_u16, delivery_tag.to_u64, false)
      end
    end
  end
end
