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
          AMQP::Frame.decode(@socket) do |frame|
            @log.debug { "Read socket #{frame.inspect}" }
            case frame
            when AMQP::HeaderFrame
              @on_frame.try &.call(frame)
              true
            when AMQP::Basic::Deliver
              @on_frame.try &.call(frame)
              true
            when AMQP::BodyFrame
              @on_frame.try &.call(frame)
              after_publish unless @ack_mode == AckMode::OnConfirm
              true
            when AMQP::Basic::Cancel
              unless frame.no_wait
                write AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
              end
              write AMQP::Connection::Close.new(320_u16, "Consumer cancelled", 0_u16, 0_u16)
              true
            when AMQP::Connection::Close
              @on_frame.try &.call(frame)
              write AMQP::Connection::CloseOk.new
              true
            when AMQP::Connection::CloseOk
              false
            else
              raise UnexpectedFrame.new(frame)
            end
          end || break
        end
      rescue ex : IO::Error | Errno | AMQP::FrameDecodeError
        @log.info "Consumer closed due to: #{ex.inspect}"
        @done.send(true) unless @done.closed?
      ensure
        @log.debug "Closing socket"
        @socket.close
      end

      def forward(frame)
        @log.debug { "Read internal #{frame.inspect}" }
        case frame
        when AMQP::Basic::Ack
          unless @ack_mode == AckMode::NoAck
            write frame
          end
          after_publish
        when AMQP::Basic::Reject
          write frame
        else
          @log.warn { "Unexpected frame: #{frame.inspect}" }
        end
      end

      private def after_publish
        @message_counter += 1
        if @source.delete_after == DeleteAfter::QueueLength &&
           @message_count <= @message_counter
          write AMQP::Connection::Close.new(320_u16, "Shovel done",
            0_u16, 0_u16)
          @done.send(false) unless @done.closed?
        end
      end

      private def set_prefetch
        write AMQP::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false)
        AMQP::Frame.decode(@socket) { |f| f.as(AMQP::Basic::QosOk) }
      end

      private def consume
        queue_name = @source.queue || ""
        passive = !queue_name.empty?
        write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, passive,
          false, true, true, false,
          {} of String => AMQP::Field)
        frame = AMQP::Frame.decode(@socket) { |f| f.as?(AMQP::Queue::DeclareOk) || raise UnexpectedFrame.new(f) }
        queue = frame.queue_name
        @message_count = frame.message_count
        if @source.exchange
          write AMQP::Queue::Bind.new(1_u16, 0_u16, frame.queue_name,
            @source.exchange.not_nil!,
            @source.exchange_key || "",
            false,
            {} of String => AMQP::Field)
          AMQP::Frame.decode(@socket) { |f| f.as?(AMQP::Queue::BindOk) || raise UnexpectedFrame.new(f) }
        end
        no_ack = @ack_mode == AckMode::NoAck
        write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, no_ack, false, false,
          {} of String => AMQP::Field)
        AMQP::Frame.decode(@socket) { |f| f.as?(AMQP::Basic::ConsumeOk) || raise UnexpectedFrame.new(f) }
      end
    end
  end
end
