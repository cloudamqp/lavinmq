require "../amqp"
require "./consts"

module LavinMQ
  module MQTT
    # Identifies an MQTT subscription within an exchange's `@bindings`.
    #
    # Mirrors the interface of `LavinMQ::AMQP::BindingKey` (`routing_key`,
    # `arguments`, `properties_key`) so that `MQTT::SubscriptionDetails` and
    # `AMQP::BindingDetails` stay interchangeable through duck typing, while keeping
    # MQTT decoupled from the AMQP binding key type.
    #
    # The subscription topic filter is carried in `topic_filter` (also exposed as
    # `routing_key` for interface parity); the QoS is carried in `qos`. `arguments`
    # exposes the QoS as the shared, read-only `AMQP::Table` the rest of the system
    # expects (`QOS_HEADER`); QoS 0 has no arguments.
    struct SubscriptionKey
      getter topic_filter : String
      getter qos : UInt8

      def initialize(@topic_filter : String, @qos : UInt8 = 0u8)
      end

      # Kept for parity with `LavinMQ::AMQP::BindingKey#routing_key` (duck typing).
      def routing_key : String
        @topic_filter
      end

      def arguments : AMQP::Table?
        if @qos.zero?
          QOS0_ARGUMENTS
        else
          QOS1_ARGUMENTS
        end
      end

      def properties_key
        return "~" if topic_filter.empty?
        "#{topic_filter}~#{@qos}"
      end
    end
  end
end
