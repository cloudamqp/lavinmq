require "amqp-client"
require "./destination"

module LavinMQ
  module Shovel
    class AMQPDestination < Destination
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil, @ack_mode = DEFAULT_ACK_MODE, direct_user : Auth::User? = nil)
        if @uri.user.nil? && @uri.host.to_s.empty?
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
        @exchange_key = @exchange_key.presence
        @queue = @queue.presence

        # @exchange == "" (the default exchange) is kept distinct from nil (unset), so
        # "dest-exchange": "" with a routing key publishes to the default exchange.
        # If neither a queue nor an exchange is configured, @exchange stays nil and
        # push falls back to the source message's original exchange and routing key.
        # dest-exchange-key only applies when dest-exchange is set
        if q = @queue
          @exchange = ""
          @exchange_key = q
        elsif @exchange.nil?
          @exchange_key = nil
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
        ch = @ch || raise "Not started"
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
