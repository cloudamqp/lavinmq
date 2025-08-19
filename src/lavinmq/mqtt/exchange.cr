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

      @bindings = Hash(BindingKey, Set(Destination)).new do |h, k|
        h[k] = Set(Destination).new
      end
      @tree = MQTT::SubscriptionTree(Destination).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String, @retain_store : MQTT::RetainStore)
        super(vhost, name, false, false, true)
      end

      def publish(packet : MQTT::Publish) : UInt32
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
        delivered_to = Set(LavinMQ::Destination).new

        # First, handle MQTT sessions using topic pattern matching
        @tree.each_entry(packet.topic) do |destination, qos|
          # Use the subscription QoS for MQTT sessions (as per MQTT spec)
          msg.properties.delivery_mode = qos
          case destination
          when MQTT::Session
            if destination.publish(msg)
              count += 1
              msg.body_io.rewind
              delivered_to.add(destination)
            end
          when LavinMQ::Queue
            if destination.publish(msg)
              count += 1
              msg.body_io.rewind
              delivered_to.add(destination)
            end
          end
        end

        # Also handle AMQP destinations via bindings
        queues = Set(LavinMQ::Queue).new
        exchanges = Set(LavinMQ::Exchange).new
        find_queues(packet.topic, msg.properties.headers, queues, exchanges)

        queues.each do |queue|
          next if delivered_to.includes?(queue) # Skip if already handled via MQTT tree
          msg.properties.delivery_mode = packet.qos
          if queue.publish(msg)
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |binding_key, ds|
          ds.each.map do |d|
            BindingDetails.new(name, vhost.name, binding_key.inner, d)
          end
        end
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        # Collect destinations from MQTT subscription tree (handles MQTT topic matching)
        destinations = Set(LavinMQ::Destination).new
        @tree.each_entry(routing_key) do |destination, qos|
          destinations.add(destination)
        end

        # Also collect destinations from AMQP-style bindings
        # For MQTT->AMQP bindings, we need exact routing key match
        @bindings.each do |binding_key, binding_destinations|
          if binding_key.inner.routing_key == routing_key
            binding_destinations.each do |destination|
              destinations.add(destination)
            end
          end
        end

        # Now yield all collected destinations
        destinations.each do |destination|
          yield destination
        end
      end

      def bind(destination : Destination, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        binding_key = BindingKey.new(routing_key, arguments)
        @bindings[binding_key].add destination
        @tree.subscribe(routing_key, destination, qos)

        data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        rk_bindings = @bindings[binding_key]
        rk_bindings.delete destination
        @bindings.delete binding_key if rk_bindings.empty?

        @tree.unsubscribe(routing_key, destination)

        data = BindingDetails.new(name, vhost.name, binding_key.inner, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
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
