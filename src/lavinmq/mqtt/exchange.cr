require "../amqp/exchange"
require "./consts"
require "./publish_headers"
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

      # `publisher` is the publishing client's session name, needed to resolve
      # the No Local subscription option [MQTT-3.8.3-3].
      def publish(packet : Protocol::Publish, publisher : String) : UInt32
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
        count = 0u32
        @tree.each_entry(topic) do |queue, options, _filter|
          # No Local [MQTT-3.8.3-3]. Bit first: the name compare is then paid
          # for only by a subscription that asked for it.
          next if options.no_local? && queue.name == publisher
          # The minimum of the publish and subscription QoS [MQTT-3.8.4-8];
          # the subscription's alone would upgrade a fire-and-forget publish.
          msg.properties.delivery_mode = Math.min(packet.qos, options.qos)
          # Retain As Published. Written for every matched entry, or a `true`
          # leaks into every later subscriber in this walk. Safe to vary per
          # destination only because MessageStore#push serializes the
          # properties synchronously, as delivery_mode above already assumes.
          headers[RETAIN_HEADER] = options.retain_as_published? if retained
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
        @tree.each_entry do |session, options, filter|
          result << SubscriptionDetails.new(name, vhost.name, SubscriptionKey.new(filter, options), session)
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
        options = MQTT.subscription_options(arguments)
        @tree.subscribe(routing_key, destination, options)

        binding_key = SubscriptionKey.new(routing_key, options)
        data = SubscriptionDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        options = MQTT.subscription_options(arguments)
        @tree.unsubscribe(routing_key, destination)

        binding_key = SubscriptionKey.new(routing_key, options)
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
