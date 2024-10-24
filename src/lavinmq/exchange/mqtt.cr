require "./exchange"
require "../mqtt/subscription_tree"
require "../mqtt/session"
require "../mqtt/retain_store"

module LavinMQ
  class MQTTExchange < Exchange
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

    def publish(packet : MQTT::Publish) : Int32
      headers = AMQP::Table.new.tap do |h|
        h["x-mqtt-retain"] = true if packet.retain?
      end
      properties = AMQP::Properties.new(headers: headers).tap do |p|
        p.delivery_mode = packet.qos if packet.responds_to?(:qos)
      end
      publish Message.new("mqtt.default", topicfilter_to_routingkey(packet.topic), String.new(packet.payload), properties), false
    end

    private def do_publish(msg : Message, immediate : Bool,
                           queues : Set(Queue) = Set(Queue).new,
                           exchanges : Set(Exchange) = Set(Exchange).new) : Int32
      count = 0
      topic = routing_key_to_topic(msg.routing_key)
      if msg.properties.try &.headers.try &.["x-mqtt-retain"]?
        @retain_store.retain(topic, msg.body_io, msg.bodysize)
      end

      @tree.each_entry(topic) do |queue, qos|
        msg.properties.delivery_mode = qos
        if queue.publish(msg)
          count += 1
          msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
        end
      end
      count
    end

    def topicfilter_to_routingkey(tf) : String
      tf.tr("/+", ".*")
    end

    def routing_key_to_topic(routing_key : String) : String
      routing_key.tr(".*", "/+")
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
      topic = routing_key_to_topic(routing_key)
      qos = headers.try { |h| h["x-mqtt-qos"]?.try(&.as(UInt8)) } || 0u8
      binding_key = MqttBindingKey.new(routing_key, headers)
      @bindings[binding_key].add destination
      @tree.subscribe(topic, destination, qos)

      data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
      notify_observers(ExchangeEvent::Bind, data)
      true
    end

    def unbind(destination : MQTT::Session, routing_key, headers = nil) : Bool
      topic = routing_key_to_topic(routing_key)
      binding_key = MqttBindingKey.new(routing_key, headers)
      rk_bindings = @bindings[binding_key]
      rk_bindings.delete destination
      @bindings.delete binding_key if rk_bindings.empty?

      @tree.unsubscribe(topic, destination)

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
