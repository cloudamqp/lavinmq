require "../amqp"
require "./consts"
require "./subscription_options"

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
    # `routing_key` for interface parity); the QoS and the v5 subscription options
    # are carried in `options`. `arguments` renders those as the `AMQP::Table` the
    # rest of the system expects, see `MQTT.subscription_arguments`. It is nilable
    # only for parity with `AMQP::BindingKey#arguments`; it never is.
    #
    # `arguments` is on the persistence path, not just the HTTP API: `compact!`
    # re-derives every binding from `bindings_details` rather than replaying the
    # original frames, so anything this method cannot reconstruct is dropped at
    # the first definitions compaction.
    struct SubscriptionKey
      getter topic_filter : String
      getter options : SubscriptionOptions

      def initialize(@topic_filter : String, @options : SubscriptionOptions = SubscriptionOptions.new)
      end

      # The QoS alone is how most callers name a subscription, so take it
      # directly rather than making every one of them wrap it.
      def initialize(topic_filter : String, qos : UInt8)
        initialize(topic_filter, SubscriptionOptions.new(qos))
      end

      def qos : UInt8
        @options.qos
      end

      # Kept for parity with `LavinMQ::AMQP::BindingKey#routing_key` (duck typing).
      def routing_key : String
        @topic_filter
      end

      def arguments : AMQP::Table?
        MQTT.subscription_arguments(@options)
      end

      # Deliberately the filter and QoS only, not the full option set: this is
      # the subscription's identity in the HTTP API, and widening it would change
      # every existing binding's URL. Two keys differing only by an option
      # therefore share a properties_key while being distinct values - which
      # cannot arise through MQTT, since a session has one binding per filter.
      #
      # Note the QoS here is whatever the key was built with, un-clamped, while
      # `arguments` clamps to `MAX_QOS`. That split is intentional and spec'd.
      def properties_key
        return "~" if topic_filter.empty?
        "#{topic_filter}~#{qos}"
      end
    end
  end
end
