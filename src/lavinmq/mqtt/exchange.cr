require "../exchange"
require "./consts"
require "./subscription_tree"
require "./session"
require "./subscription_key"
require "./subscription_details"

module LavinMQ
  module MQTT
    class Exchange
      include Stats
      include SortableJSON

      @tree = MQTT::SubscriptionTree(MQTT::Session).new

      def type : String
        "mqtt"
      end

      getter vhost, name

      rate_stats({"publish_in", "publish_out", "unroutable", "dedup"})

      def initialize(@vhost : VHost, @name : String)
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
        each_subscription do |session, topic_filter, qos|
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(topic_filter, qos), session)
        end
        result
      end

      def binding_count : Int32
        @tree.size
      end

      # MQTT-native subscription entry points. `bind`/`unbind` are the
      # AMQP-shaped adapters over these, still in use while subscriptions are
      # persisted as AMQP binding frames.
      def subscribe(session : MQTT::Session, topic_filter : String, qos : UInt8) : Bool
        @tree.subscribe(topic_filter, session, qos)
        true
      end

      def unsubscribe(session : MQTT::Session, topic_filter : String) : Bool
        @tree.unsubscribe(topic_filter, session)
        true
      end

      # Yields every subscription as session, topic filter and granted QoS.
      # The block is captured: `SubscriptionTree#each_entry` captures its own.
      def each_subscription(&block : (MQTT::Session, String, UInt8) ->) : Nil
        @tree.each_entry do |session, qos, topic_filter|
          block.call(session, topic_filter, qos)
        end
      end

      # TODO: notify observers of ExchangeEvent::Bind/Unbind once MQTT has its
      # own observable events. The payload to send is the SubscriptionDetails
      # for `destination` and `routing_key`, as `bindings_details` builds them.
      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        subscribe(destination, routing_key, MQTT.qos(arguments))
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        unsubscribe(destination, routing_key)
      end

      def bind(_destination : LavinMQ::AMQP::Destination, _rk : String, _args = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(name)
      end

      def details_tuple
        {
          name: @name, type: type, durable: true, auto_delete: false,
          internal: true, arguments: nil, vhost: @vhost.name,
          policy: nil,
          operator_policy: nil,
          effective_policy_definition: nil,
          message_stats: current_stats_details,
          effective_arguments: nil,
        }
      end
    end
  end
end
