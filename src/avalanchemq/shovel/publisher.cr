require "../connection"

module AvalancheMQ
  class Shovel
    class Publisher < Connection
      def initialize(@destination : Destination, @ack_mode : AckMode, log : Logger, @done : Channel(Bool))
        @log = log.dup
        @log.progname += " publisher"
        @message_count = 0_u64
        @delivery_tags = Hash(UInt64, UInt64).new
        @last_delivery_tag = 0_u64
        @last_message_size = 0_u64
        @last_message_pos = 0_u64
        super(@destination.uri, @log)
      end

      @on_frame : Proc(AMQP::Frame, Nil)?

      def on_frame(&blk : AMQP::Frame -> Nil)
        @on_frame = blk
      end

      def run
        set_confirm if @ack_mode == AckMode::OnConfirm
        spawn(read_loop, name: "Shovel publisher #{@destination.uri.host}#read_loop")
      end

      private def read_loop
        loop do
          AMQP::Frame.from_io(@socket) do |frame|
            @log.debug { "Read from socket #{frame.inspect}" }
            case frame
            when AMQP::Frame::Basic::Ack
              handle_ack(frame)
              true
            when AMQP::Frame::Basic::Nack
              handle_nack(frame)
              true
            when AMQP::Frame::Basic::Return
              reject(@message_count) if @ack_mode == AckMode::OnConfirm
              true
            when AMQP::Frame::Connection::Close
              write AMQP::Frame::Connection::CloseOk.new
              false
            when AMQP::Frame::Connection::CloseOk
              false
            else true
            end
          end || break
        end
      rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
        @log.info "Closed due to: #{ex.inspect}"
      ensure
        @log.debug "Closing socket"
        # @done will be closed if the shovel is actually done, so we can always try to send true
        @done.send(true) unless @done.closed?
        @socket.close unless @socket.closed?
      end

      private def handle_ack(frame)
        return unless @ack_mode == AckMode::OnConfirm
        if frame.multiple
          with_multiple(frame.delivery_tag) { |t| ack(t) }
        else
          ack(@delivery_tags[frame.delivery_tag])
        end
      end

      private def handle_nack(frame)
        return unless @ack_mode == AckMode::OnConfirm
        if frame.multiple
          with_multiple(frame.delivery_tag) { |t| reject(t) }
        else
          reject(@delivery_tags[frame.delivery_tag])
        end
      end

      def forward(frame)
        case frame
        when AMQP::Frame::Basic::Deliver
          send_basic_publish(frame)
        when AMQP::Frame::Header
          @last_message_size = frame.body_size
          @last_message_pos = 0_u64
          @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
          if @last_message_size.zero?
            @socket.flush
            if @ack_mode == AckMode::OnPublish && @last_message_pos >= @last_message_size
              ack(@last_delivery_tag)
            end
          end
        when AMQP::Frame::Body
          @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
          @socket.flush
          @last_message_pos += frame.body_size
          if @ack_mode == AckMode::OnPublish && @last_message_pos >= @last_message_size
            ack(@last_delivery_tag)
          end
        else
          @log.warn { "Unexpected frame: #{frame.inspect}" }
        end
      end

      private def send_basic_publish(frame)
        exchange = @destination.exchange.not_nil!
        routing_key = @destination.exchange_key || frame.routing_key
        mandatory = @ack_mode == AckMode::OnConfirm
        pframe = AMQP::Frame::Basic::Publish.new(frame.channel,
          0_u16,
          exchange,
          routing_key,
          mandatory,
          false)
        @socket.write_bytes pframe, ::IO::ByteFormat::NetworkEndian
        case @ack_mode
        when AckMode::OnConfirm
          @message_count += 1
          @delivery_tags[@message_count] = frame.delivery_tag
        when AckMode::OnPublish
          @last_delivery_tag = frame.delivery_tag
        end
      end

      private def set_confirm
        write AMQP::Frame::Confirm::Select.new(1_u16, false)
        @socket.flush
        AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Confirm::SelectOk) }
      end

      private def with_multiple(delivery_tag)
        @delivery_tags.delete_if do |m, t|
          next false unless m <= delivery_tag
          yield t
          true
        end
      end

      private def ack(delivery_tag)
        @on_frame.try &.call(AMQP::Frame::Basic::Ack.new(1_u16, delivery_tag, false))
      end

      private def reject(delivery_tag)
        @on_frame.try &.call(AMQP::Frame::Basic::Reject.new(1_u16, delivery_tag, false))
      end
    end
  end
end
