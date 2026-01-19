require "./destination"

module LavinMQ
  module Shovel
    class AMQPDestination < Destination
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil, @ack_mode = DEFAULT_ACK_MODE, direct_user : Auth::User? = nil)
        unless @uri.user
          if direct_user
            @uri.user = direct_user.name
            @uri.password = direct_user.plain_text_password
          else
            raise ArgumentError.new("direct_user required")
          end
        end
        params = @uri.query_params
        params["name"] ||= "Shovel #{@name} sink"
        @uri.query = params.to_s
        if queue
          @exchange = ""
          @exchange_key = queue
        end
        if @exchange.nil?
          raise ArgumentError.new("Shovel destination requires an exchange")
        end
      end

      def start
        return if started?
        if c = @conn
          c.close
        end
        conn = ::AMQP::Client.new(@uri).connect
        @conn = conn
        @ch = ch = conn.channel
        if q = @queue
          begin
            ch.queue_declare(q, passive: true)
          rescue ::AMQP::Client::Channel::ClosedException
            @ch = ch = conn.channel
            ch.queue_declare(q, passive: false)
          end
        end
      end

      def stop
        @conn.try &.close
        @ch = nil
      end

      def started? : Bool
        !@ch.nil? && !@conn.try &.closed?
      end

      def push(msg, source)
        raise "Not started" unless started?
        ch = @ch.not_nil!
        ex = @exchange || msg.exchange
        rk = @exchange_key || msg.routing_key
        case @ack_mode
        in AckMode::OnConfirm
          ch.basic_publish(msg.body_io, ex, rk, props: msg.properties) do
            source.ack(msg.delivery_tag)
          end
        in AckMode::OnPublish
          ch.basic_publish(msg.body_io, ex, rk, props: msg.properties)
          source.ack(msg.delivery_tag)
        in AckMode::NoAck
          ch.basic_publish(msg.body_io, ex, rk, props: msg.properties)
        end
      end
    end
  end
end
