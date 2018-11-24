require "logger"
require "../client/direct_client"

module AvalancheMQ
  module Federation
    class Upstream
      class Publisher < DirectClient
        @log : Logger

        def initialize(@upstream : Upstream, @federated_q : Queue)
          @log = @upstream.log.dup
          @log.progname += " publisher"
          client_properties = {
            "connection_name" => "Federation #{@upstream.name}",
          } of String => AMQP::Field
          @message_count = 0_u64
          @delivery_tags = Hash(UInt64, UInt64).new
          @last_delivery_tag = 0_u64
          @last_message_size = 0_u64
          @last_message_pos = 0_u64
          super(@upstream.vhost, client_properties)
        end

        @on_frame : Proc(AMQP::Frame, Nil)?

        def on_frame(&blk : AMQP::Frame -> Nil)
          @on_frame = blk
        end

        def run
          set_confirm if @upstream.ack_mode == AckMode::OnConfirm
        end

        def handle_frame(frame)
          case frame
          when AMQP::Frame::Basic::Ack
            return unless @upstream.ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| ack(t) }
            else
              ack(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Frame::Basic::Nack
            return unless @upstream.ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| reject(t) }
            else
              reject(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Frame::Basic::Return
            reject(@message_count) if @upstream.ack_mode == AckMode::OnConfirm
          when AMQP::Frame::Connection::Close
            write AMQP::Frame::Connection::CloseOk.new
          else
            @log.debug { "No action for #{frame.inspect}" }
          end
        end

        def forward(frame)
          case frame
          when AMQP::Frame::Basic::Deliver
            send_basic_publish(frame, @federated_q.name)
          when AMQP::Frame::Header
            write(frame)
            @last_message_size = frame.body_size
            @last_message_pos = 0_u64
            if @last_message_size.zero? && @upstream.ack_mode == AckMode::OnPublish
              ack(@last_delivery_tag)
            end
          when AMQP::Frame::Body
            write(frame)
            @last_message_pos += frame.body_size
            if @upstream.ack_mode == AckMode::OnPublish && @last_message_pos >= @last_message_size
              ack(@last_delivery_tag)
            end
          else
            @log.warn { "Unexpected frame: #{frame.inspect}" }
          end
        end

        def send_basic_publish(frame, routing_key = nil)
          exchange = ""
          mandatory = @upstream.ack_mode == AckMode::OnConfirm
          pframe = AMQP::Frame::Basic::Publish.new(frame.channel, 0_u16, exchange, routing_key,
            mandatory, false)
          write pframe
          if mandatory
            @delivery_tags[@message_count += 1] = frame.delivery_tag
          elsif @upstream.ack_mode == AckMode::OnPublish
            @last_delivery_tag = frame.delivery_tag
          end
        end

        private def set_confirm
          write AMQP::Frame::Confirm::Select.new(1_u16, false)
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
end
