require "../message"
require "../amqp/exchange"
require "./topic_translator"

module LavinMQ
  module MQTT
    # Adapts a downstream `AMQP::Exchange` so it can be held in the MQTT
    # `SubscriptionTree` alongside sessions. When an MQTT message matches the
    # binding filter, the wrapper translates the topic into an AMQP routing key
    # and routes the message into the wrapped exchange — enabling MQTT
    # exchange-to-AMQP-exchange bindings (#1136).
    #
    # Equality is defined on the wrapped exchange so bind/unbind can create
    # fresh wrappers: two wrappers over the same downstream exchange are
    # interchangeable as tree entries.
    class AMQPDestination
      getter exchange : LavinMQ::AMQP::Exchange
      getter arguments : AMQP::Table?

      def initialize(@exchange : LavinMQ::AMQP::Exchange, @arguments : AMQP::Table? = nil)
      end

      # Equality is intentionally on the wrapped exchange only (not arguments),
      # so unbind can match with a fresh wrapper regardless of arguments.
      def_equals_and_hash @exchange

      # Translates the MQTT topic to an AMQP routing key and routes the message
      # into the wrapped exchange via its full publish path (so downstream
      # dedup/delayed/alternate-exchange semantics apply). Returns whether the
      # message was routed to at least one downstream queue. The new `Message`
      # shares the original body IO; the downstream routing rewinds it.
      def publish(msg : Message) : Bool
        routing_key = TopicTranslator.mqtt_to_amqp(msg.routing_key)
        translated = Message.new(msg.timestamp, @exchange.name, routing_key,
          msg.properties, msg.bodysize, msg.body_io)
        @exchange.publish(translated, false).routed?
      end
    end
  end
end
