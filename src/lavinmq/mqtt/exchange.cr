require "../amqp/exchange"
require "./consts"
require "./publish_headers"
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

      # `publisher` is the publishing client's session name, needed to resolve
      # the No Local subscription option [MQTT-3.8.3-3].
      def publish(packet : Protocol::Publish, publisher : String, ctx : PublishContext) : UInt32
        ctx.reset
        @publish_in_count.add(1, :relaxed)
        headers = AMQP::Table.new
        # Reserve the slot before the properties, and always as a Bool, so the
        # per-subscription overwrite below stays on Table's in-place path and
        # scans one key rather than up to six v5 property fields.
        retained = packet.retain?
        headers[RETAIN_HEADER] = false if retained
        PublishHeaders.store(packet.properties, headers)
        properties = AMQP::Properties.new(headers: headers)
        properties.delivery_mode = packet.qos

        timestamp = RoughTime.unix_ms
        bodysize = packet.payload.bytesize.to_u64
        body = ::IO::Memory.new(packet.payload, writable: false)

        # `Publish#topic` decodes @topic into a fresh String on every call, so
        # hold it once: this is the publish hot path.
        topic = packet.topic

        msg = Message.new(timestamp, EXCHANGE, topic, properties, bodysize, body)
        msg.needs_sync = packet.qos > 0
        count = 0u32
        @tree.each_entry(topic) do |subscriber, options, filter|
          # No Local [MQTT-3.8.3-3]. Bit first: the name compare is then paid
          # for only by a subscription that asked for it.
          next if options.no_local? && subscriber.is_a?(Session) && subscriber.name == publisher
          # The lower of the publish and the subscription QoS [MQTT-3.8.4-8].
          msg.properties.delivery_mode = Math.min(packet.qos, options.qos)
          # Retain As Published. Written for every matched entry, or a `true`
          # leaks into every later subscriber in this walk. Safe to vary per
          # destination only because MessageStore#push serializes the
          # properties synchronously, as delivery_mode above already assumes.
          headers[RETAIN_HEADER] = options.retain_as_published? if retained
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
        @tree.each_entry do |subscriber, options, filter|
          next unless session = subscriber.as?(MQTT::Session)
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(filter, options), session)
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
        options = MQTT.subscription_options(arguments)
        # Before routing to it, so the session's storage is ready
        return false unless destination.subscription_bound(routing_key, options)
        @tree.subscribe(routing_key, destination, options)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        @tree.unsubscribe(routing_key, destination)
        destination.subscription_unbound(routing_key)
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
