require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      # In MQTT only topic/routing key is used in routing, but arguments is used to
      # store QoS level for each subscription. To make @bingings treat the same subscription
      # with different QoS as the same subscription this "custom" BindingKey is used which
      # only includes the routing key in the hash.
      struct BindingKey
        def initialize(routing_key : String, arguments : AMQP::Table? = nil)
          @binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        end

        def inner
          @binding_key
        end

        def hash
          @binding_key.routing_key.hash
        end
      end

      @bindings = Hash(BindingKey, Set(MQTT::Session)).new do |h, k|
        h[k] = Set(MQTT::Session).new
      end
      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, true, false, true)
        @body = ::IO::Memory.new
      end

      def publish(msg : Message, immediate : Bool,
                  queues : Set(Queue) = Set(Queue).new,
                  exchanges : Set(Exchange) = Set(Exchange).new) : Int32
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def publish(packet : MQTT::Publish) : Int32
        @publish_in_count += 1
        properties = AMQP::Properties.new(headers: AMQP::Table.new)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        @body.clear
        @body.write(packet.payload)
        @body.rewind

        if packet.retain?
          @retain_store.retain(packet.topic, @body, bodysize)
          @body.rewind
        end

        msg = Message.new(timestamp, EXCHANGE, packet.topic, properties, bodysize, @body)
        count = 0
        @tree.each_entry(packet.topic) do |queue, qos|
          msg.properties.delivery_mode = qos
          if queue.publish(msg)
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count += 1 if count.zero?
        @publish_out_count += count
        count
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |binding_key, ds|
          ds.each.map do |d|
            BindingDetails.new(name, vhost.name, binding_key.inner, d)
          end
        end
      end

      # Only here to make superclass happy
      protected def bindings(routing_key, headers) : Iterator(LavinMQ::Destination)
        Iterator(LavinMQ::Destination).empty
      end

      def bind(destination : MQTT::Session, routing_key : String, headers = nil) : Bool
        qos = headers.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        binding_key = BindingKey.new(routing_key, headers)
        @bindings[binding_key].add destination
        @tree.subscribe(routing_key, destination, qos)

        data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, headers = nil) : Bool
        binding_key = BindingKey.new(routing_key, headers)
        rk_bindings = @bindings[binding_key]
        rk_bindings.delete destination
        @bindings.delete binding_key if rk_bindings.empty?

        @tree.unsubscribe(routing_key, destination)

        data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      def bind(destination : Destination, routing_key : String, headers = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination : Destination, routing_key, headers = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?)
      end

      def clear_policy
      end

      def handle_arguments
      end
    end
  end
end
