require "../exchange"
require "./subscription_tree"
require "./session"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < Exchange
      struct MqttBindingKey
        def initialize(routing_key : String, arguments : AMQP::Table? = nil)
          @binding_key = BindingKey.new(routing_key, arguments)
        end

        def inner
          @binding_key
        end

        def hash
          @binding_key.routing_key.hash
        end
      end

      @bindings = Hash(MqttBindingKey, Set(MQTT::Session)).new do |h, k|
        h[k] = Set(MQTT::Session).new
      end
      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, true, false, true)
      end

      def publish(msg : Message, immediate : Bool,
                  queues : Set(Queue) = Set(Queue).new,
                  exchanges : Set(Exchange) = Set(Exchange).new) : Int32
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def publish(packet : MQTT::Publish) : Int32
        @publish_in_count += 1

        headers = AMQP::Table.new.tap do |h|
          h["x-mqtt-retain"] = true if packet.retain?
        end
        properties = AMQP::Properties.new(headers: headers).tap do |p|
          p.delivery_mode = packet.qos if packet.responds_to?(:qos)
        end

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.size.to_u64
        body = ::IO::Memory.new(bodysize)
        body.write(packet.payload)
        body.rewind

        @retain_store.retain(packet.topic, body, bodysize) if packet.retain?

        body.rewind
        msg = Message.new(timestamp, "mqtt.default", packet.topic, properties, bodysize, body)

        count = 0
        @tree.each_entry(packet.topic) do |queue, qos|
          msg.properties.delivery_mode = qos
          if queue.publish(msg)
            count += 1
            msg.body_io.seek(-msg.bodysize.to_i64, ::IO::Seek::Current) # rewind
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
      protected def bindings(routing_key, headers) : Iterator(Destination)
        Iterator(Destination).empty
      end

      def bind(destination : MQTT::Session, routing_key : String, headers = nil) : Bool
        qos = headers.try { |h| h["x-mqtt-qos"]?.try(&.as(UInt8)) } || 0u8
        binding_key = MqttBindingKey.new(routing_key, headers)
        @bindings[binding_key].add destination
        @tree.subscribe(routing_key, destination, qos)

        data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, headers = nil) : Bool
        binding_key = MqttBindingKey.new(routing_key, headers)
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
    end
  end
end
