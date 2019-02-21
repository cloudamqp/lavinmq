require "../connection"

module AvalancheMQ
  class Shovel
    class Consumer < Connection
      def initialize(@source : Source, @ack_mode : AckMode, log : Logger, @done : Channel(Bool))
        @log = log.dup
        @log.progname += " consumer"
        @message_counter = 0_u32
        @message_count = 0_u32
        super(@source.uri, @log)
      end

      @on_frame : Proc(AMQP::Frame, Nil)?

      def on_frame(&blk : AMQP::Frame -> Nil)
        @on_frame = blk
      end

      def run
        set_prefetch
        consume
        spawn(read_loop, name: "Shovel consumer #{@source.uri.host}#read_loop")
      end

      private def read_loop
        loop do
          AMQP::Frame.from_io(@socket) do |frame|
            @log.debug { "Read socket #{frame.inspect}" }
            case frame
            when AMQP::Frame::Basic::Deliver, AMQP::Frame::Header
              @on_frame.try &.call(frame)
              true
            when AMQP::Frame::Body
              @on_frame.try &.call(frame)
              after_publish unless @ack_mode == AckMode::OnConfirm
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
            when AMQP::Frame::Channel::Flow
              write AMQP::Frame::Channel::FlowOk.new frame.channel, frame.active
              true
            else
              raise UnexpectedFrame.new(frame)
            end
          end || break
        end
      rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
        @log.info "Consumer closed due to: #{ex.inspect}"
      ensure
        @log.debug "Closing socket"
        # @done will be closed if the shovel is actually done, so we can always try to send true
        @done.send(true) unless @done.closed?
        @socket.close unless @socket.closed?
      end

      def forward(frame)
        @log.debug { "Read internal #{frame.inspect}" }
        case frame
        when AMQP::Frame::Basic::Ack
          unless @ack_mode == AckMode::NoAck
            write frame
          end
          after_publish
        when AMQP::Frame::Basic::Reject
          write frame
        else
          @log.warn { "Unexpected frame: #{frame.inspect}" }
        end
      end

      private def after_publish
        @message_counter += 1
        if @source.delete_after == DeleteAfter::QueueLength &&
           @message_count <= @message_counter
          @log.debug { "Queue length #{@message_count} reached (#{@message_counter}), closing" }
          write AMQP::Frame::Connection::Close.new(320_u16, "Shovel done", 0_u16, 0_u16)
          @done.send(false) unless @done.closed?
        end
      end

      private def set_prefetch
        write AMQP::Frame::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false)
        AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::QosOk) }
      end

      private def consume
        queue_name = @source.queue || ""
        passive = !queue_name.empty?
        write AMQP::Frame::Queue::Declare.new(1_u16, 0_u16, queue_name, passive,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Queue::DeclareOk) }
        queue = frame.queue_name
        @message_count = frame.message_count
        @log.debug { "Consuming #{@message_count} from #{queue_name}" }
        if @source.exchange
          write AMQP::Frame::Queue::Bind.new(1_u16, 0_u16, frame.queue_name,
            @source.exchange.not_nil!,
            @source.exchange_key || "",
            false,
            {} of String => AMQP::Field)
          AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Queue::BindOk) }
        end
        no_ack = @ack_mode == AckMode::NoAck
        write AMQP::Frame::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, no_ack, false, false,
          {} of String => AMQP::Field)
        AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Basic::ConsumeOk) }
      end
    end
  end
end
