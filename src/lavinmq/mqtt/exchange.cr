require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./subscription_details"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      # The vhost's tree, shared with every `x-mqtt-topic` exchange in it.
      @tree : MQTT::SubscriptionTree(MQTT::Subscriber)

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, false, false, true)
        @tree = vhost.mqtt_subscriptions
      end

      def publish(packet : Protocol::Publish) : UInt32
        @publish_in_count.add(1, :relaxed)
        properties = AMQP::Properties.new(headers: AMQP::Table.new)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writable: false)

        if packet.retain?
          @retain_store.retain(packet.topic, body, bodysize)
          body.rewind
        end

        msg = Message.new(timestamp, EXCHANGE, packet.topic, properties, bodysize, body)
        count = 0u32
        @tree.each_entry(packet.topic) do |subscriber, qos, filter|
          msg.properties.delivery_mode = qos
          if subscriber.deliver(msg, filter)
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      # Only MQTT sessions: the tree is shared, and the entries belonging to an
      # `x-mqtt-topic` exchange are that exchange's bindings, not ours.
      def bindings_details : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        @tree.each_entry do |subscriber, qos, filter|
          next unless subscriber.is_a?(MQTT::Session)
          arguments = AMQP::Table.new
          arguments[QOS_HEADER] = qos
          result << SubscriptionDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, arguments), subscriber)
        end
        result
      end

      def binding_count : Int32
        count = 0
        @tree.each_entry do |subscriber, _qos, _filter|
          count += 1 if subscriber.is_a?(MQTT::Session)
        end
        count
      end

      # Only here to make superclass happy
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        @tree.subscribe(routing_key, destination, qos)

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        @tree.unsubscribe(routing_key, destination)

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        # Our own bindings, not `@tree.empty?`: the tree is shared with the
        # vhost's `x-mqtt-topic` exchanges.
        delete if @auto_delete && binding_count.zero?
        true
      end

      def bind(destination : Destination, routing_key : String, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      private def apply_policy_argument(key : String, value : JSON::Any)
        # mqtt exchange doesn't support policies, make this a noop
      end

      private def clear_policy_arguments
        # mqtt exchange doesn't support policies, make this a noop
      end

      def handle_arguments
        # mqtt exchange doesn't support arguments, make this a noop
      end
    end
  end
end
