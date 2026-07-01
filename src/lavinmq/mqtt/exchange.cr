require "sync/shared"
require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      # The subscription tree is the single source of truth for MQTT routing.
      # Wrapped in Sync::Shared so concurrent publishes take the read lock while
      # subscribe/unsubscribe take the exclusive lock.
      @tree : Sync::Shared(MQTT::SubscriptionTree(MQTT::Session)) = Sync::Shared.new(
        MQTT::SubscriptionTree(MQTT::Session).new, :checked)

      def type : String
        "mqtt"
      end

      def bindings_lock_holder : Fiber?
        @tree.locked_by_fiber
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
        @tree.shared do |tree|
          tree.each_entry(packet.topic) do |queue, qos, _filter|
            msg.properties.delivery_mode = qos
            if queue.publish(msg)
              count += 1
              msg.body_io.rewind
            end
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      def bindings_details : Array(BindingDetails)
        result = Array(BindingDetails).new
        @tree.shared do |tree|
          tree.each_entry do |session, qos, filter|
            arguments = AMQP::Table.new
            arguments[QOS_HEADER] = qos
            result << BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, arguments), session)
          end
        end
        result
      end

      def binding_count : Int32
        @tree.unsafe_get.size
      end

      # Only here to make superclass happy
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        @tree.lock(&.subscribe(routing_key, destination, qos))

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        all_empty = false
        @tree.lock do |tree|
          tree.unsubscribe(routing_key, destination)
          all_empty = tree.empty?
        end

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && all_empty
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
