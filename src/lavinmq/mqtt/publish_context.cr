require "../amqp/destination"
require "../amqp/routing_context"

module LavinMQ
  module MQTT
    class PublishContext
      record Outcome, handled : Bool, counted_unroutable : Bool

      getter routing = AMQP::RoutingContext.new
      getter outcomes = Hash(AMQP::Exchange, Outcome).new

      def reset : self
        @outcomes.clear
        self
      end
    end
  end
end
