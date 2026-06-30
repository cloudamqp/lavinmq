require "../exchange"
require "../amqp/exchange"
require "../stats"
require "../observable"
require "../policy"
require "../sortable_json"
require "./consts"
require "../destination"
require "./subscription_tree"
require "./session"
require "./amqp_destination"
require "./retain_store"

module LavinMQ
  module MQTT
    # Entries held in the MQTT SubscriptionTree: either an MQTT session or a
    # wrapper routing into a downstream AMQP exchange (#1136).
    alias Subscriber = Session | AMQPDestination

    # The MQTT routing exchange. It deliberately does NOT inherit from
    # `AMQP::Exchange`: MQTT routing has its own topic semantics (slash
    # separated levels, '+'/'#' wildcards) and its own message ingress
    # (`broker.publish(packet)`), so it only implements the protocol-agnostic
    # `LavinMQ::Exchange` contract plus the MQTT-specific routing.
    class Exchange < LavinMQ::Exchange
      include Stats
      include PolicyTarget
      include SortableJSON
      include Observable(ExchangeEvent)

      Log = ::LavinMQ::Log.for "mqtt.exchange"

      getter name : String
      getter vhost : VHost
      getter arguments = AMQP::Table.new
      getter? durable = false
      getter? auto_delete = false
      getter? internal = true

      # Match the AMQP exchange stat surface so polymorphic consumers (the
      # Prometheus exporter, management UI) work; "dedup" stays 0 for MQTT.
      rate_stats({"publish_in", "publish_out", "unroutable", "dedup"})

      @tree = MQTT::SubscriptionTree(MQTT::Subscriber).new
      @deleted = false

      def type : String
        "mqtt"
      end

      def initialize(@vhost : VHost, @name : String, @retain_store : MQTT::RetainStore)
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
        @tree.each_entry(packet.topic) do |entry, qos, _filter|
          # Sessions carry the per-subscription QoS in delivery_mode; for an AMQP
          # destination, map the publish QoS to AMQP persistence semantics
          # (QoS>=1 -> persistent) so durable downstream queues persist it (#1136).
          routed =
            case entry
            in MQTT::Session
              msg.properties.delivery_mode = qos
              entry.publish(msg)
            in MQTT::AMQPDestination
              msg.properties.delivery_mode = packet.qos >= 1 ? 2u8 : 1u8
              entry.publish(msg)
            end
          if routed
            count += 1
            msg.body_io.rewind
          end
        end
        @unroutable_count.add(1, :relaxed) if count.zero?
        @publish_out_count.add(count, :relaxed)
        count
      end

      # The MQTT exchange is not a valid target for AMQP-side publishing; the
      # only ingress is `publish(packet)`. Satisfies the `LavinMQ::Exchange`
      # contract used by `VHost#publish`.
      def publish(msg : Message, immediate : Bool,
                  queues : Set(AMQP::Queue) = Set(AMQP::Queue).new,
                  exchanges : Set(LavinMQ::Exchange) = Set(LavinMQ::Exchange).new) : AMQP::Exchange::PublishResult
        AMQP::Exchange::PublishResult::None
      end

      # No-op: the MQTT exchange never contributes AMQP queues when reached as a
      # destination of an AMQP exchange. Exists so the virtual `find_queues`
      # dispatch over `LavinMQ::Exchange` compiles.
      def find_queues(routing_key : String, headers : AMQP::Table?,
                      queues : Set(AMQP::Queue) = Set(AMQP::Queue).new,
                      exchanges : Set(LavinMQ::Exchange) = Set(LavinMQ::Exchange).new) : Nil
      end

      # Not a valid AMQP routing target; satisfies the virtual `route_msg`
      # dispatch over `LavinMQ::Exchange`.
      def route_msg(msg : Message) : AMQP::Exchange::PublishResult
        AMQP::Exchange::PublishResult::None
      end

      def bindings_details : Array(BindingDetails)
        result = Array(BindingDetails).new
        @tree.each_entry do |entry, qos, filter|
          case entry
          in MQTT::Session
            arguments = AMQP::Table.new
            arguments[QOS_HEADER] = qos
            result << BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, arguments), entry)
          in MQTT::AMQPDestination
            # Report the wrapped downstream exchange as the binding destination,
            # preserving the arguments supplied at bind time.
            result << BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, entry.arguments), entry.exchange)
          end
        end
        result
      end

      def binding_count : Int32
        @tree.size
      end

      # Only the cross-protocol exchange-to-exchange bindings (to durable
      # destinations) are persisted; session subscriptions stay transient.
      # Built lazily so compaction never materializes a BindingDetails per MQTT
      # session subscription (#1136).
      def bindings_to_persist : Array(BindingDetails)
        result = Array(BindingDetails).new
        @tree.each_entry do |entry, _qos, filter|
          next unless entry.is_a?(MQTT::AMQPDestination)
          dest = entry.exchange
          next unless dest.durable?
          result << BindingDetails.new(name, vhost.name, LavinMQ::BindingKey.new(filter, entry.arguments), dest)
        end
        result
      end

      # The internal MQTT exchange is never (re)declarable by an AMQP client, so
      # it never matches a declare frame.
      def match?(frame : AMQP::Frame) : Bool
        false
      end

      def match?(type, durable, auto_delete, internal, arguments) : Bool
        false
      end

      def bind(destination : MQTT::Session, routing_key : String, arguments = nil) : Bool
        qos = arguments.try { |h| h[QOS_HEADER]?.try(&.as(UInt8)) } || 0u8
        @tree.subscribe(routing_key, destination, qos)

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : MQTT::Session, routing_key, arguments = nil) : Bool
        @tree.unsubscribe(routing_key, destination)

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @tree.empty?
        true
      end

      # Bind a downstream AMQP exchange so MQTT publishes matching the filter
      # are routed into it (cross-protocol exchange-to-exchange, #1136). The
      # routing key is an MQTT-syntax topic filter, matched against MQTT topics.
      def bind(destination : LavinMQ::AMQP::Exchange, routing_key : String, arguments = nil) : Bool
        @tree.subscribe(routing_key, AMQPDestination.new(destination, arguments), 0u8)

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : LavinMQ::AMQP::Exchange, routing_key, arguments = nil) : Bool
        @tree.unsubscribe(routing_key, AMQPDestination.new(destination))

        binding_key = LavinMQ::BindingKey.new(routing_key, arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        true
      end

      def bind(destination : Destination, routing_key : String, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      # The MQTT exchange is always recreated at boot, so bindings from it can
      # be persisted and replayed even though it isn't durable (#1136).
      def persistent? : Bool
        true
      end

      def in_use? : Bool
        !@tree.empty?
      end

      # No-op: the retain store is owned and closed by the `Broker`.
      def close : Nil
      end

      def delete : Nil
        return if @deleted
        @deleted = true
        @vhost.delete_exchange(@name)
        notify_observers(ExchangeEvent::Deleted)
      end

      def details_tuple
        {
          name:                        @name,
          type:                        type,
          durable:                     durable?,
          auto_delete:                 auto_delete?,
          internal:                    internal?,
          arguments:                   @arguments,
          vhost:                       @vhost.name,
          policy:                      policy.try &.name,
          operator_policy:             operator_policy.try &.name,
          effective_policy_definition: Policy.merge_definitions(policy, operator_policy),
          message_stats:               current_stats_details,
          effective_arguments:         Array(String).new,
        }
      end

      def to_json(json : JSON::Builder)
        json.object do
          details_tuple.merge(message_stats: stats_details).each do |k, v|
            json.field(k, v) unless v.nil?
          end
        end
      end

      private def apply_policy_argument(key : String, value : JSON::Any) : Bool
        # mqtt exchange doesn't support policies, make this a noop
        false
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
