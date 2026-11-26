require "json"
require "../sortable_json"
require "../policy"
require "../amqp/binding_details"
require "../amqp/binding_key"
require "../mqtt/consts"
require "../mqtt/exchange"
require "../mqtt/session"
require "../mqtt/subscription_details"

module LavinMQ
  module HTTP
    # The HTTP API showed MQTT objects as AMQP ones: the MQTT exchange as one
    # of the vhost's exchanges, and a session with a default exchange binding.
    # API clients expect that until a major release removes it.
    module MQTTExchangeCompat
      extend self

      # `vhost.exchanges`, with the MQTT exchange first
      def exchanges(vhost : VHost) : Array(Exchange | MQTTExchangeView)
        result = Array(Exchange | MQTTExchangeView).new(exchanges_size(vhost))
        each_exchange(vhost) { |e| result << e }
        result
      end

      # `vhost.each_exchange`, with the MQTT exchange first
      def each_exchange(vhost : VHost, & : Exchange | MQTTExchangeView ->) : Nil
        yield MQTTExchangeView.new(vhost)
        vhost.each_exchange { |e| yield e }
      end

      def exchanges_size(vhost : VHost) : Int32
        vhost.exchanges_size + 1
      end

      # `queue.bindings`. A session was listed with a default exchange
      # binding too, though the default exchange never routes to sessions.
      def queue_bindings(queue : Queue)
        return queue.bindings unless queue.is_a?(MQTT::Session)
        result = Array(AMQP::BindingDetails | MQTT::SubscriptionDetails).new
        result << AMQP::BindingDetails.new("", queue.vhost.name, AMQP::BindingKey.new(queue.name), queue)
        result.concat(queue.bindings)
      end
    end

    # The MQTT exchange in the shape of an `AMQP::Exchange`
    struct MQTTExchangeView
      include SortableJSON

      def initialize(@vhost : VHost)
      end

      def name : String
        MQTT::EXCHANGE
      end

      def bindings_details : Array(MQTT::SubscriptionDetails)
        exchange.bindings_details
      end

      def binding_count : Int32
        exchange.binding_count
      end

      def dedup_count : UInt64
        exchange.dedup_count
      end

      # `name` last, as in AMQP exchanges' JSON: DefaultExchange merges it in,
      # and Crystal gives all exchange tuples that order
      def details_tuple
        policy, operator_policy = @vhost.exchange_policies(name)
        {
          type: "mqtt", durable: false, auto_delete: false, internal: true,
          arguments: AMQP::Table.new, vhost: @vhost.name,
          policy: policy.try &.name,
          operator_policy: operator_policy.try &.name,
          effective_policy_definition: Policy.merge_definitions(policy, operator_policy),
          message_stats: exchange.current_stats_details,
          effective_arguments: Array(String).new,
          name: name,
        }
      end

      # As `AMQP::Exchange#to_json`
      def to_json(json : JSON::Builder)
        json.object do
          details_tuple.merge(message_stats: exchange.stats_details).each do |k, v|
            json.field(k, v) unless v.nil?
          end
        end
      end

      protected def search_value
        name
      end

      private def exchange : MQTT::Exchange
        @vhost.mqtt_exchange
      end
    end
  end
end
