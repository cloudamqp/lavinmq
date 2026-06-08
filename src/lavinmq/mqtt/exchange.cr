require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./retain_store"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      @tree = MQTT::SubscriptionTree(MQTT::Session).new
      # session => {routing_key => binding arguments}, reverse index so a session's
      # subscriptions can be looked up and listed without scanning every binding.
      # In MQTT only the routing key identifies a subscription; arguments only carry QoS.
      @session_bindings = Hash(MQTT::Session, Hash(String, AMQP::Table?)).new do |h, k|
        h[k] = Hash(String, AMQP::Table?).new
      end

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

      def bindings_details : Array(BindingDetails)
        @session_bindings.flat_map do |session, rks|
          rks.map do |rk, args|
            BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(rk, args), session)
          end
        end
      end

      def subscription(session : MQTT::Session, routing_key : String) : {Bool, AMQP::Table?}
        if rks = @session_bindings[session]?
          if rks.has_key?(routing_key)
            return {true, rks[routing_key]}
          end
        end
        {false, nil}
      end

      def bindings_details_for(destination) : Array(BindingDetails)
        session = destination.as?(MQTT::Session)
        return [] of BindingDetails unless session
        rks = @session_bindings[session]?
        return [] of BindingDetails unless rks
        rks.map do |rk, args|
          BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(rk, args), session)
        end
      end

      # Only here to make superclass happy
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        @tree.subscribe(routing_key, destination, qos)
        @session_bindings[destination][routing_key] = arguments

        data = BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(routing_key, arguments), destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        if rks = @session_bindings[destination]?
          rks.delete routing_key
          @session_bindings.delete destination if rks.empty?
        end

        @tree.unsubscribe(routing_key, destination)

        data = BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(routing_key, arguments), destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @session_bindings.empty?
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
