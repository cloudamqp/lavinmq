require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./subscription_key"
require "./subscription_details"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      @bindings = Hash(SubscriptionKey, Set(MQTT::Session)).new do |h, k|
        h[k] = Set(MQTT::Session).new
      end
      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, false, false, true)
      end

      def publish(packet : Protocol::Publish) : UInt32
        @publish_in_count.add(1, :relaxed)
        properties = AMQP::Properties.new(headers: AMQP::Table.new)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writeable: false)

        if packet.retain?
          @retain_store.retain(packet.topic, body, bodysize)
          body.rewind
        end

        msg = Message.new(timestamp, EXCHANGE, packet.topic, properties, bodysize, body)
        count = 0u32
        @tree.each_entry(packet.topic) do |queue, qos|
          msg.properties.delivery_mode = qos
          if queue.publish(msg).ok?
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      def bindings_details : Array(SubscriptionDetails)
        @bindings.flat_map do |binding_key, ds|
          ds.map do |d|
            SubscriptionDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def binding_count : Int32
        @bindings.each_value.sum(&.size)
      end

      # Only here to make superclass happy
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        binding_key = SubscriptionKey.new(routing_key, qos)
        @bindings[binding_key].add destination
        @tree.subscribe(routing_key, destination, qos)

        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        binding_key = SubscriptionKey.new(routing_key, qos)
        rk_bindings = @bindings[binding_key]
        rk_bindings.delete destination
        @bindings.delete binding_key if rk_bindings.empty?

        @tree.unsubscribe(routing_key, destination)

        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
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
