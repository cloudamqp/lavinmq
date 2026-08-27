require "./exchange"
require "../../mqtt/subscriber"
require "../../mqtt/subscription_tree"

module LavinMQ
  module AMQP
    # An exchange that AMQP queues bind to using MQTT topic filter syntax, so
    # that an MQTT publish to `a/b/c` reaches a queue bound with `a/b/#`,
    # `a/+/c` and so on. Routing keys are MQTT topics verbatim, slashes
    # included; nothing is translated.
    #
    # The exchange itself is the entry in the vhost's `SubscriptionTree`,
    # registered once per distinct binding key rather than once per bound
    # queue. `MQTT::Exchange#publish` walks that tree once for every subscriber
    # in the vhost and hands back the filter that matched, which `#deliver`
    # looks up in the binding book below.
    #
    # See docs/adr/0001-mqtt-topic-exchange.md.
    class MqttTopicExchange < Exchange
      include MQTT::Subscriber

      # The sole source of truth for this exchange's bindings. The tree can't
      # provide it: it is shared with every other subscriber in the vhost and
      # holds filters, not filters x destinations.
      @bindings = Hash(String, Set({AMQP::Destination, BindingKey})).new do |h, k|
        h[k] = Set({AMQP::Destination, BindingKey}).new
      end
      @tree : MQTT::SubscriptionTree(MQTT::Subscriber)

      def type : String
        "x-mqtt-topic"
      end

      def initialize(vhost : VHost, name : String, durable = false,
                     auto_delete = false, internal = false,
                     arguments = AMQP::Table.new)
        # Internal regardless of what was asked for: an AMQP publish has no MQTT
        # topic to route on, and with a no-op `each_destination` a publisher
        # would get silence rather than an error. `#match?` ignores the flag so
        # that a redeclare with internal: false is still idempotent.
        super(vhost, name, durable, auto_delete, true, arguments)
        @tree = vhost.mqtt_subscriptions
      end

      def match?(type, durable, auto_delete, internal, arguments) : Bool
        super(type, durable, auto_delete, @internal, arguments)
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

      def bind(destination : AMQP::Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        destinations = @bindings[routing_key]
        first = destinations.empty?
        return false unless destinations.add?({destination, binding_key})
        # One tree entry per filter, however many destinations are bound to it.
        # QoS 1 because that's the strongest MQTT offers here; it only matters
        # for the sessions sharing the tree, never for us.
        @tree.subscribe(routing_key, self, 1u8) if first
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil)
        destinations = @bindings[routing_key]? || return false
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless destinations.delete({destination, binding_key})
        if destinations.empty?
          @bindings.delete(routing_key)
          @tree.unsubscribe(routing_key, self)
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        # Our own bindings, never `@tree.empty?`: the tree is shared.
        delete if @auto_delete && @bindings.empty?
        true
      end

      # `MQTT::Subscriber`: an MQTT publish matched `filter`, one of our binding
      # keys.
      def deliver(msg : Message, filter : String) : Bool
        destinations = @bindings[filter]? || return false
        @publish_in_count.add(1, :relaxed)
        # A fresh message rather than the one we were handed: that one carries
        # `mqtt.default` as its exchange name and has its delivery_mode
        # rewritten in place for every entry the tree walk yields. `Message` and
        # `Properties` are both structs, so this allocates nothing.
        #
        # delivery_mode 2 unconditionally: persistence is derived from queue
        # durability and this field is never read, so it's metadata only.
        properties = AMQP::Properties.new(delivery_mode: 2u8)
        outgoing = Message.new(msg.timestamp, @name, msg.routing_key, properties,
          msg.bodysize, msg.body_io)
        count = 0u32
        destinations.each do |destination, _binding_key|
          routed = case destination
                   in AMQP::Queue    then destination.publish(outgoing).ok?
                   in AMQP::Exchange then destination.route_msg(outgoing).routed?
                   end
          count += 1 if routed
          outgoing.body_io.rewind
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count.positive?
      end

      # Nothing routes in over AMQP: publishing here is refused, and MQTT
      # publishes arrive through `#deliver`. This is also what makes the
      # exchange unreachable through `find_queues` if it's bound as the
      # destination of a public exchange.
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      protected def delete
        @bindings.each_key { |filter| @tree.unsubscribe(filter, self) }
        @bindings.clear
        super
      end

      def handle_arguments
        # no arguments are supported, make this a noop
      end

      private def apply_policy_argument(key : String, value : JSON::Any)
        # no policies are supported, make this a noop
      end

      private def clear_policy_arguments
        # no policies are supported, make this a noop
      end
    end
  end
end
