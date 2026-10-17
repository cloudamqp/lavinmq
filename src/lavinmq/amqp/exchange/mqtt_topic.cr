require "./exchange"
require "../../mqtt/subscriber"
require "../../mqtt/topic_filter"

module LavinMQ
  module AMQP
    # Routes MQTT publishes to AMQP destinations. Queues and exchanges are
    # bound with MQTT topic filter syntax ("a/+/c", "a/b/#"), and a delivered
    # message's routing key is the MQTT topic verbatim.
    #
    # The exchange registers itself in the vhost's SubscriptionTree, once per
    # distinct binding key, and fans out to its own bindings on deliver.
    class MqttTopicExchange < Exchange
      include MQTT::Subscriber

      # Topic filter => bindings. Both levels are replaced, never mutated, so
      # MQTT publishes on other threads deliver without locks.
      @bindings = CowMap(String, BindingSet).new
      # Serializes bind, unbind and delete, so that the exchange's entry in
      # the subscription tree follows the first and last binding of a filter
      @bindings_lock = Mutex.new

      def type : String
        "x-mqtt-topic"
      end

      # internal is forced: AMQP publishes can't enter this exchange, only
      # MQTT publishes reach it, through the subscription tree.
      def initialize(vhost : VHost, name : String, durable = false,
                     auto_delete = false, internal = false,
                     arguments = AMQP::Table.new)
        super(vhost, name, durable, auto_delete, true, arguments)
      end

      # internal is forced true in the constructor, so ignore the flag here to
      # keep a redeclare with internal: false idempotent.
      def match?(type, durable, auto_delete, internal, arguments)
        super(type, durable, auto_delete, true, arguments)
      end

      def bindings_details : Array(BindingDetails)
        details = Array(BindingDetails).new
        @bindings.each do |(_, bindings)|
          bindings.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
        details
      end

      def binding_count : Int32
        count = 0
        @bindings.each_value { |bindings| count += bindings.size }
        count
      end

      def bind(destination : AMQP::Destination, routing_key, arguments = nil) : Bool
        unless MQTT::TopicFilter.valid_filter?(routing_key)
          raise LavinMQ::Error::PreconditionFailed.new("'#{routing_key}' is not a valid MQTT topic filter")
        end
        binding_key = BindingKey.new(routing_key, arguments)
        @bindings_lock.synchronize do
          current = @bindings[routing_key]?
          bindings = (current || BindingSet.empty).add(destination, binding_key) || return false
          @bindings[routing_key] = bindings
          @vhost.mqtt_subscription_tree.subscribe(routing_key, self, 1u8) unless current
        end
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_bound(data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        @bindings_lock.synchronize do
          current = @bindings[routing_key]? || return false
          bindings = current.delete(destination, binding_key)
          return false if bindings.same?(current)
          if bindings.empty?
            @bindings.delete(routing_key)
            @vhost.mqtt_subscription_tree.unsubscribe(routing_key, self)
          else
            @bindings[routing_key] = bindings
          end
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_unbound(data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      # A fresh Message struct: the one the tree walk hands out is shared and
      # its delivery_mode is rewritten per entry. delivery_mode 2 is metadata
      # only; persistence is derived from queue durability.
      #
      # The tree calls this once per matching filter, so a publish matched by
      # several filters arrives several times with the same context. It is
      # counted in publish_in once, and in unroutable at most once. The
      # per-publish state lives in the context, on the publishing fiber, so a
      # publish parked in a queue's store lock is not disturbed by another
      # client's publish into this exchange.
      def deliver(msg : Message, filter : String, ctx : MQTT::PublishContext) : Bool
        bindings = @bindings[filter]? || return false
        outcome = ctx.outcomes[self]?
        publish_in_count_add! unless outcome
        outcome ||= MQTT::PublishContext::Outcome.new(handled: false, counted_unroutable: false)
        properties = AMQP::Properties.new
        properties.delivery_mode = 2u8
        message = Message.new(msg.timestamp, name, msg.routing_key, properties, msg.bodysize, msg.body_io)
        message.needs_sync = msg.needs_sync?
        count = 0u32
        rejected = false
        bindings.each_destination do |destination|
          case destination
          in AMQP::Queue
            case destination.publish(message)
            in .ok?                 then count += 1
            in .overflow?, .closed? then rejected = true
            in .dropped?            then nil
            end
          in AMQP::Exchange
            count += 1 if destination.route_msg(message, ctx.queues, ctx.exchanges).routed?
          end
          message.body_io.rewind
        end
        publish_out_count_add!(count)
        ctx.outcomes[self] = count_unroutable(outcome, handled: count.positive? || rejected)
        count.positive?
      end

      # Like Exchange#route_msg: unroutable when no destination accepted the
      # publish and none rejected it (overflow or a closed queue). A later
      # filter of the same publish can still route it, so an earlier count is
      # taken back then.
      private def count_unroutable(outcome : MQTT::PublishContext::Outcome, handled : Bool) : MQTT::PublishContext::Outcome
        if handled
          unroutable_count_sub!(1) if outcome.counted_unroutable
          MQTT::PublishContext::Outcome.new(handled: true, counted_unroutable: false)
        elsif !outcome.handled && !outcome.counted_unroutable
          unroutable_count_add!
          MQTT::PublishContext::Outcome.new(handled: false, counted_unroutable: true)
        else
          outcome
        end
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        # MQTT publishes enter through #deliver; nothing routes through here.
      end

      protected def delete
        return if @deleted
        @bindings_lock.synchronize do
          @bindings.each_key do |filter|
            @vhost.mqtt_subscription_tree.unsubscribe(filter, self)
          end
        end
        super
      end

      private def apply_policy_argument(key : String, value : JSON::Any)
        # policies aren't supported, make this a noop
      end

      private def clear_policy_arguments
        # policies aren't supported, make this a noop
      end

      def handle_arguments
        # arguments aren't supported, make this a noop
      end
    end
  end
end
