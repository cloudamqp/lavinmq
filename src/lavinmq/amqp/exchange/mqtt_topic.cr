require "./exchange"
require "../../mqtt/subscriber"
require "../../mqtt/topic_filter"

module LavinMQ
  module AMQP
    # Routes MQTT publishes to AMQP destinations. Queues and exchanges are
    # bound with MQTT topic filter syntax ("a/+/c", "a/b/#"), and a delivered
    # message's routing key is the MQTT topic verbatim.
    #
    # The exchange registers itself in the vhost's SubscriptionTree, once per
    # distinct binding key, and fans out to its own bindings on deliver.
    class MqttTopicExchange < Exchange
      include MQTT::Subscriber

      @bindings = Hash(String, Set({AMQP::Destination, BindingKey})).new do |h, k|
        h[k] = Set({AMQP::Destination, BindingKey}).new
      end

      def type : String
        "x-mqtt-topic"
      end

      # internal is forced: AMQP publishes can't enter this exchange, only
      # MQTT publishes reach it, through the subscription tree.
      def initialize(vhost : VHost, name : String, durable = false,
                     auto_delete = false, internal = false,
                     arguments = AMQP::Table.new)
        super(vhost, name, durable, auto_delete, true, arguments)
      end

      # internal is forced true in the constructor, so ignore the flag here to
      # keep a redeclare with internal: false idempotent.
      def match?(type, durable, auto_delete, internal, arguments)
        super(type, durable, auto_delete, true, arguments)
      end

      def bindings_details : Array(BindingDetails)
        @bindings.flat_map do |_filter, destinations|
          destinations.map do |destination, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, destination)
          end
        end
      end

      def binding_count : Int32
        @bindings.each_value.sum(&.size)
      end

      def bind(destination : AMQP::Destination, routing_key, arguments = nil) : Bool
        unless MQTT::TopicFilter.valid_filter?(routing_key)
          raise LavinMQ::Error::PreconditionFailed.new("'#{routing_key}' is not a valid MQTT topic filter")
        end
        binding_key = BindingKey.new(routing_key, arguments)
        destinations = @bindings[routing_key]
        first_for_filter = destinations.empty?
        return false unless destinations.add?({destination, binding_key})
        @vhost.mqtt_subscription_tree.subscribe(routing_key, self, 1u8) if first_for_filter
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil) : Bool
        destinations = @bindings[routing_key]? || return false
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless destinations.delete({destination, binding_key})
        if destinations.empty?
          @bindings.delete(routing_key)
          @vhost.mqtt_subscription_tree.unsubscribe(routing_key, self)
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      # A fresh Message struct: the one the tree walk hands out is shared and
      # its delivery_mode is rewritten per entry. delivery_mode 2 is metadata
      # only; persistence is derived from queue durability.
      def deliver(msg : Message, filter : String) : Bool
        destinations = @bindings[filter]? || return false
        @publish_in_count.add(1, :relaxed)
        properties = AMQP::Properties.new
        properties.delivery_mode = 2u8
        message = Message.new(msg.timestamp, name, msg.routing_key, properties, msg.bodysize, msg.body_io)
        message.needs_sync = msg.needs_sync?
        count = 0u32
        destinations.each do |destination, _binding_key|
          case destination
          in AMQP::Queue
            count += 1 if destination.publish(message).ok?
          in AMQP::Exchange
            count += 1 if destination.route_msg(message).routed?
          end
          message.body_io.rewind
        end
        @publish_out_count.add(count, :relaxed)
        @unroutable_count.add(1, :relaxed) if count.zero?
        count.positive?
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        # MQTT publishes enter through #deliver; nothing routes through here.
      end

      protected def delete
        return if @deleted
        @bindings.each_key do |filter|
          @vhost.mqtt_subscription_tree.unsubscribe(filter, self)
        end
        super
      end

      private def apply_policy_argument(key : String, value : JSON::Any)
        # policies aren't supported, make this a noop
      end

      private def clear_policy_arguments
        # policies aren't supported, make this a noop
      end

      def handle_arguments
        # arguments aren't supported, make this a noop
      end
    end
  end
end
