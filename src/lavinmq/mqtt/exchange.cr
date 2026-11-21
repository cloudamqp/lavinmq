require "./consts"
require "./subscription_tree"
require "./session"
require "./subscription_key"
require "./subscription_details"
require "./publish_context"

module LavinMQ
  module MQTT
    class Exchange
      include Stats

      @tree : MQTT::SubscriptionTree(MQTT::Subscriber)

      getter vhost, name

      rate_stats({"publish_in", "publish_out", "unroutable", "dedup"})

      def initialize(@vhost : VHost, @name : String)
        @tree = vhost.mqtt_subscription_tree
      end

      def publish(packet : Protocol::Publish, ctx : PublishContext) : UInt32
        ctx.reset
        @publish_in_count.add(1, :relaxed)
        @vhost.count_mqtt_published(packet.qos)
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

      def bindings_details : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        each_subscription do |session, topic_filter, qos|
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(topic_filter, qos), session)
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

      def subscribe(session : MQTT::Session, topic_filter : String, qos : UInt8) : Bool
        @tree.subscribe(topic_filter, session, qos)
        session.subscription_bound(topic_filter, qos)
        true
      end

      def unsubscribe(session : MQTT::Session, topic_filter : String) : Bool
        @tree.unsubscribe(topic_filter, session)
        session.subscription_unbound(topic_filter)
        true
      end

      # Walks the session's own filters rather than the tree, and changes the
      # session's hash only after the walk
      def unsubscribe_all(session : MQTT::Session) : Nil
        session.each_subscription { |topic_filter, _qos| @tree.unsubscribe(topic_filter, session) }
        session.clear_subscriptions
      end

      # Captured, not yielded: `SubscriptionTree#each_entry` captures its own.
      # The tree is shared with x-mqtt-topic exchanges, so only sessions are
      # this exchange's subscriptions.
      def each_subscription(&block : (MQTT::Session, String, UInt8) ->) : Nil
        @tree.each_entry do |subscriber, qos, topic_filter|
          next unless session = subscriber.as?(MQTT::Session)
          block.call(session, topic_filter, qos)
        end
      end
    end
  end
end
