require "./destination"
require "../endpoint/session"

module LavinMQ
  module Shovel
    # Publishes to an exchange, or a queue through the default exchange, on an
    # Endpoint: this broker in-process or another broker over AMQP.
    class AMQPDestination < Destination
      @started = false

      def initialize(@name : String, @session : Endpoint::Session, @queue : String?,
                     @exchange : String? = nil, @exchange_key : String? = nil,
                     @ack_mode = DEFAULT_ACK_MODE)
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
        @session.close
        @session.open
        if q = @queue
          begin
            @session.declare_queue(q, passive: true)
          rescue Endpoint::NotFound
            @session.declare_queue(q, passive: false)
          end
        end
        @started = true
      rescue ex
        @session.close
        raise ex
      end

      def stop
        @started = false
        @session.close
      end

      def started? : Bool
        @started && !@session.closed?
      end

      def push(msg) : Nil
        raise "Not started" unless started?
        ex = @exchange || msg.exchange
        rk = @exchange_key || msg.routing_key
        tag = msg.tag
        case @ack_mode
        in AckMode::OnConfirm
          # The confirm is the broker's ack/nack. A nack (e.g. reject-publish
          # overflow) is transient — the queue may drain — so it becomes Retry,
          # never a silent ack. When the session closes every pending confirm
          # is voided with a nack as well. That is reported as Retry too: when
          # the destination goes away on its own the source is still open and
          # every in-flight message has to go back to it, or a later cumulative
          # ack would settle it undelivered. When the whole shovel is stopping
          # the source is already closed and the Runner ignores the report.
          listener = @listener
          session = @session
          generation = session.generation
          session.publish(ex, rk, msg.properties, msg.body) do |confirmed|
            # A confirm from before a restart: the source was restarted too,
            # its delivery tags name other messages now
            next if session.generation != generation
            listener.report(tag, confirmed ? Outcome::Confirmed : Outcome::Retry)
          end
        in AckMode::OnPublish
          @session.publish(ex, rk, msg.properties, msg.body)
          @listener.report(tag, Outcome::Confirmed)
        in AckMode::NoAck
          @session.publish(ex, rk, msg.properties, msg.body)
        end
      end
    end
  end
end
