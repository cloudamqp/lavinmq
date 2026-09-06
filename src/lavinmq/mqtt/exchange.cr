require "../amqp/exchange"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./subscription_key"
require "./subscription_details"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String)
        super(vhost, name, false, false, true)
      end

      def publish(packet : Protocol::Publish) : UInt32
        @publish_in_count.add(1, :relaxed)
        properties = AMQP::Properties.new(headers: AMQP::Table.new)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writable: false)

        msg = Message.new(timestamp, EXCHANGE, packet.topic, properties, bodysize, body)
        count = 0u32
        @tree.each_entry(packet.topic) do |queue, qos, _filter|
          msg.properties.delivery_mode = qos
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
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(filter, qos), session)
        end
        result
      end

      def binding_count : Int32
        @tree.size
      end

      # Only here to make superclass happy
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
      end

      # Only an MQTT session may bind here, and the check is a runtime one on
      # purpose.
      #
      # The obvious spelling is two overloads, one restricted to
      # `MQTT::Session` and one to `Destination` that refuses. That does not
      # work: `Destination` is an alias union that *contains* `MQTT::Session`,
      # so both overloads match a session and which one Crystal picks depends
      # on the order the definitions were compiled in, which depends on the
      # require graph. Embedding LavinMQ as a shard and requiring
      # `mqtt/broker` before `server` was enough to flip it, and the symptom
      # was every SUBSCRIBE failing with "Access refused to mqtt.default"
      # while CONNECT still worked.
      #
      # One overload and an `is_a?` cannot be reordered into being wrong.
      def bind(destination : Destination, routing_key : String, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self) unless destination.is_a?(MQTT::Session)

        qos = MQTT.qos(arguments)
        @tree.subscribe(routing_key, destination, qos)

        binding_key = SubscriptionKey.new(routing_key, qos)
        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self) unless destination.is_a?(MQTT::Session)

        qos = MQTT.qos(arguments)
        @tree.unsubscribe(routing_key, destination)

        binding_key = SubscriptionKey.new(routing_key, qos)
        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @tree.empty?
        true
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
