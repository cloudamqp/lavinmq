require "logger"
require "../client/direct_client"

module AvalancheMQ
  class Upstream
    class Publisher
      @client : DirectClient
      @log : Logger

      def initialize(@upstream : Upstream)
        @log = @upstream.log.dup
        @log.progname += " publisher"
        @out_ch = Channel::Buffered(AMQP::Frame).new(@upstream.prefetch.to_i32)
        client_properties = {
          "connection_name" => "Federation #{@upstream.name}",
        } of String => AMQP::Field
        @client = @upstream.vhost.direct_client(@out_ch, client_properties)
        set_confirm if @upstream.ack_mode == AckMode::OnConfirm
        @message_count = 0_u32
        @delivery_tags = Hash(UInt64, UInt64).new
      end

      def start(@consumer : Consumer)
        client_read_loop
      end

      private def client_read_loop
        spawn(name: "Upstream publisher #{@upstream.name}#client_read_loop") do
          loop do
            Fiber.yield if @out_ch.empty?
            frame = @out_ch.receive
            @log.debug { "Read #{frame.inspect}" }
            case frame
            when AMQP::Basic::Nack
              next unless @upstream.ack_mode == AckMode::OnConfirm
              if frame.multiple
                with_multiple(frame.delivery_tag) { |t| @consumer.not_nil!.reject(t) }
              else
                @consumer.not_nil!.reject(@delivery_tags[frame.delivery_tag])
              end
            when AMQP::Basic::Return
              @consumer.not_nil!.reject(@message_count) if @upstream.ack_mode == AckMode::OnConfirm
            when AMQP::Basic::Ack
              next unless @upstream.ack_mode == AckMode::OnConfirm
              if frame.multiple
                with_multiple(frame.delivery_tag) { |t| @consumer.not_nil!.ack(t) }
              else
                @consumer.not_nil!.ack(@delivery_tags[frame.delivery_tag])
              end
            when AMQP::Connection::CloseOk
              break
            when AMQP::Connection::Close
              @client.write AMQP::Connection::CloseOk.new
              break
            when AMQP::Basic::Cancel
              @client.write AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
            when AMQP::Channel::Close
              @client.write AMQP::Channel::CloseOk.new(frame.channel)
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue ex : Channel::ClosedError
            @log.debug { "#client_read_loop closed" }
            close
            break
          end
        end
      end

      private def set_confirm
        @client.write AMQP::Confirm::Select.new(1_u16, false)
        @out_ch.receive.as(AMQP::Confirm::SelectOk)
      end

      private def with_multiple(delivery_tag)
        @delivery_tags.delete_if do |m, t|
          next false unless m <= delivery_tag
          yield t
          true
        end
      end

      def send_basic_publish(frame, routing_key = nil)
        exchange = ""
        mandatory = @upstream.ack_mode == AckMode::OnConfirm
        pub_frame = AMQP::Basic::Publish.new(frame.channel, 0_u16, exchange, routing_key,
          mandatory, false)
        @log.debug "Send #{pub_frame.inspect}"
        @client.write pub_frame
        if mandatory
          @message_count += 1
          @delivery_tags[@message_count.to_u64] = frame.delivery_tag
        elsif @upstream.ack_mode == AckMode::OnPublish
          @consumer.not_nil!.ack(frame.delivery_tag)
        end
      end

      def write(frame)
        @client.write(frame)
      end

      def close
        return if @client.closed?
        @client.write AMQP::Connection::Close.new(320_u16, "Federation stopped", 0_u16, 0_u16)
      end
    end
  end
end
