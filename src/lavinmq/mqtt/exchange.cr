require "../amqp/exchange"
require "./consts"
require "./subscription_tree"
require "./session"
require "./subscription_key"
require "./subscription_details"
require "./publish_context"

module LavinMQ
  module MQTT
    class Exchange < AMQP::Exchange
      @tree : MQTT::SubscriptionTree(MQTT::Subscriber)

      def type : String
        "mqtt"
      end

      def initialize(vhost : VHost, name : String)
        @tree = vhost.mqtt_subscription_tree
        super(vhost, name, false, false, true)
      end

      def publish(packet : Protocol::Publish, ctx : PublishContext) : UInt32
        ctx.reset
        @publish_in_count.add(1, :relaxed)
        properties = AMQP::Properties.new(headers: AMQP::Table.new)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writable: false)

        msg = Message.new(timestamp, EXCHANGE, packet.topic, properties, bodysize, body)
        msg.needs_sync = packet.qos > 0
        count = 0u32
        @tree.each_entry(packet.topic) do |subscriber, qos, filter|
          msg.properties.delivery_mode = qos
          if subscriber.deliver(msg, filter, ctx)
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      # The tree is shared with x-mqtt-topic exchanges, so only MQTT::Session
      # entries are this exchange's own bindings.
      def bindings_details : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        @tree.each_entry do |subscriber, qos, filter|
          next unless session = subscriber.as?(MQTT::Session)
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(filter, qos), session)
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
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        @tree.subscribe(routing_key, destination, MQTT.qos(arguments))
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        @tree.unsubscribe(routing_key, destination)
        delete if @auto_delete && @tree.empty?
        true
      end

      def bind(destination : LavinMQ::Queue | LavinMQ::Exchange, routing_key : String, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination : LavinMQ::Queue | LavinMQ::Exchange, routing_key, arguments = nil) : Bool
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
