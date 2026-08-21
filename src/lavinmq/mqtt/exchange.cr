require "../amqp/exchange"
require "./consts"
require "./publish_headers"
require "../destination"
require "./subscription_tree"
require "./session"
require "./subscription_details"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, false, false, true)
      end

      def publish(packet : Protocol::Publish) : UInt32
        @publish_in_count.add(1, :relaxed)
        headers = AMQP::Table.new
        PublishHeaders.store(packet.properties, headers)
        properties = AMQP::Properties.new(headers: headers)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writable: false)

        # `Publish#topic` decodes @topic into a fresh String on every call, so
        # hold it once: this is the publish hot path.
        topic = packet.topic

        if packet.retain?
          @retain_store.retain(topic, body, bodysize)
          body.rewind
        end

        msg = Message.new(timestamp, EXCHANGE, topic, properties, bodysize, body)
        count = 0u32
        @tree.each_entry(topic) do |queue, qos, _filter|
          # The minimum of the publish and subscription QoS [MQTT-3.8.4-8];
          # the subscription's alone would upgrade a fire-and-forget publish.
          msg.properties.delivery_mode = Math.min(packet.qos, qos)
          if queue.publish(msg)
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      def bindings_details : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        @tree.each_entry do |session, qos, filter|
          arguments = AMQP::Table.new
          arguments[QOS_HEADER] = qos
          result << SubscriptionDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, arguments), session)
        end
        result
      end

      def binding_count : Int32
        @tree.size
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

        delete if @auto_delete && @tree.empty?
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
