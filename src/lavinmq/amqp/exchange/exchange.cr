require "../../amqp"
require "../../binding_key"
require "../../binding_details"
require "../destination"
require "../../error"
require "../../exchange"
require "../../observable"
require "../../policy"
require "../../stats"
require "../../sortable_json"
require "../queue"
require "./event"

module LavinMQ
  module AMQP
    abstract class Exchange < LavinMQ::Exchange
      include PolicyTarget
      include Stats
      include SortableJSON
      include Observable(ExchangeEvent)

      getter name, arguments, vhost, type, alternate_exchange
      getter? durable, internal, auto_delete
      getter policy : Policy?
      getter operator_policy : OperatorPolicy?
      getter? delayed = false

      @alternate_exchange : String?
      @delayed_queue : Queue?
      @deleted = false
      @deduper : Deduplication::Deduper?
      @effective_args = Array(String).new

      rate_stats({"publish_in", "publish_out", "unroutable", "dedup"})

      def initialize(@vhost : VHost, @name : String, @durable = false,
                     @auto_delete = false, @internal = false,
                     @arguments = AMQP::Table.new)
        handle_arguments
      end

      def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?)
        clear_policy
        Policy.merge_definitions(policy, operator_policy).each do |k, v|
          case k
          when "alternate-exchange"
            @alternate_exchange ||= v.as_s?
          when "delayed-message"
            @delayed ||= v.as?(Bool) == true
            init_delayed_queue if @delayed
          when "federation-upstream"
            @vhost.upstreams.try &.link(v.as_s, self) unless internal?
          when "federation-upstream-set"
            @vhost.upstreams.try &.link_set(v.as_s, self) unless internal?
          else nil
          end
        end
        @policy = policy
        @operator_policy = operator_policy
      end

      def clear_policy
        handle_arguments
        @policy = nil
        @operator_policy = nil
        @vhost.upstreams.try &.stop_link(self)
      end

      def handle_arguments
        @effective_args = Array(String).new
        @alternate_exchange = (@arguments["x-alternate-exchange"]? || @arguments["alternate-exchange"]?).try &.to_s
        @effective_args << "x-alternate-exchange" if @alternate_exchange
        if @arguments["x-delayed-exchange"]?.try &.as?(Bool)
          @delayed = true
          init_delayed_queue
          @effective_args << "x-delayed-exchange"
        end
        if @arguments["x-message-deduplication"]?.try(&.as?(Bool))
          @effective_args << "x-message-deduplication"
          ttl = parse_header("x-cache-ttl", Int).try(&.to_u32)
          @effective_args << "x-cache-ttl" if ttl
          size = parse_header("x-cache-size", Int).try(&.to_u32)
          @effective_args << "x-cache-size" if size
          header_key = parse_header("x-deduplication-header", String)
          @effective_args << "x-deduplication-header" if header_key
          cache = Deduplication::MemoryCache(AMQ::Protocol::Field).new(size)
          @deduper = Deduplication::Deduper.new(cache, ttl, header_key)
        end
      end

      private macro parse_header(header, type)
        if value = @arguments["{{ header.id }}"]?
          value.as?({{ type }}) || raise LavinMQ::Error::PreconditionFailed.new("{{ header.id }} header not a {{ type.id }}")
        end
      end

      def details_tuple
        {
          name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
          internal: @internal, arguments: @arguments, vhost: @vhost.name,
          policy: @policy.try &.name,
          operator_policy: @operator_policy.try &.name,
          effective_policy_definition: Policy.merge_definitions(@policy, @operator_policy),
          message_stats: current_stats_details,
          effective_arguments: @effective_args,
        }
      end

      def match?(frame : AMQP::Frame)
        match?(frame.exchange_type, frame.durable, frame.auto_delete, frame.internal, frame.arguments)
      end

      def match?(type, durable, auto_delete, internal, arguments)
        delayed = type == "x-delayed-message"
        frame_args = arguments
        if delayed
          frame_args = frame_args.clone.merge!({"x-delayed-exchange": true})
          frame_args.delete("x-delayed-type")
        end
        self.type == (delayed ? arguments["x-delayed-type"]? : type) &&
          @durable == durable &&
          @auto_delete == auto_delete &&
          @internal == internal &&
          @arguments == frame_args
      end

      def in_use?
        return true unless bindings_details.empty?
        @vhost.exchanges.any? do |_, x|
          x.bindings_details.any? { |bd| bd.destination == self }
        end
      end

      MAX_NAME_LENGTH = 256

      private def init_delayed_queue
        return if @delayed_queue
        return unless @delayed
        q_name = "amq.delayed.#{@name}"
        raise "Exchange name too long" if q_name.bytesize > MAX_NAME_LENGTH
        arguments = AMQP::Table.new({
          "x-dead-letter-exchange" => @name,
          "auto-delete"            => @auto_delete,
        })
        @delayed_queue = if durable?
                           AMQP::DurableDelayedExchangeQueue.new(@vhost, q_name, false, false, arguments)
                         else
                           AMQP::DelayedExchangeQueue.new(@vhost, q_name, false, false, arguments)
                         end
        @vhost.queues[q_name] = @delayed_queue.as(Queue)
      end

      REPUBLISH_HEADERS = {"x-head", "x-tail", "x-from"}

      protected def delete
        return if @deleted
        @deleted = true
        @delayed_queue.try &.delete
        @vhost.delete_exchange(@name)
        notify_observers(ExchangeEvent::Deleted)
      end

      # This outer macro will add a finished macro hook to all inherited classes
      # in LavinMQ::AMQP namespace.
      macro inherited
        {% if @type.name.starts_with?("LavinMQ::AMQP::") %}
          # This macro will find the "bind" method of classes inheriting from this class
          # and redefine them to raise AccessRefused exception if the first argument
          # isn't a type in LavinMQ::AMQP namespace.
          #
          # TODO remove this when LavinMQ::MQTT::Session no longer inherit from
          # LavinMQ::AMQP::Queue and LavinMQ::MQTT::Exchange no longer inherit from
          # lavinMQ::AMQP::Exchange
        macro finished
          \{% if (m = @type.methods.find(&.name.== "bind"))  %}
            def bind(\{{ m.args.map(&.id).join(",").id}}) : Bool
              unless \{{m.args[0].name.id}}.class.name.starts_with?("LavinMQ::AMQP::")
                raise AccessRefused.new(self)
              end
              \{{ m.body }}
          end
         \{% end %}
        end
        {% end %}
      end

      def bind(destination : LavinMQ::Destination, routing_key, arguments = nil) : Bool
        raise AccessRefused.new(self)
      end

      abstract def type : String
      abstract def bind(destination : AMQP::Destination, routing_key : String, arguments : AMQP::Table?)
      abstract def unbind(destination : AMQP::Destination, routing_key : String, arguments : AMQP::Table?)
      abstract def bindings_details : Iterator(BindingDetails)
      abstract def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)

      def publish(msg : Message, immediate : Bool,
                  queues : Set(LavinMQ::Queue) = Set(LavinMQ::Queue).new,
                  exchanges : Set(LavinMQ::Exchange) = Set(LavinMQ::Exchange).new) : Bool
        @publish_in_count.add(1, :relaxed)
        if d = @deduper
          if d.duplicate?(msg)
            @dedup_count.add(1, :relaxed)
            return false
          end
          d.add(msg)
        end
        if should_delay_message?(msg.properties.headers)
          if q = @delayed_queue
            q.publish(msg)
            @publish_out_count.add(1, :relaxed)
            return true
          else
            @unroutable_count.add(1, :relaxed)
            return false
          end
        end
        route_msg(msg, immediate, queues, exchanges)
      end

      def route_msg(msg : Message) : Bool
        route_msg(msg, false, Set(LavinMQ::Queue).new, Set(LavinMQ::Exchange).new)
      end

      private def route_msg(msg : Message, immediate : Bool, queues : Set(LavinMQ::Queue), exchanges : Set(LavinMQ::Exchange)) : Bool
        headers = msg.properties.headers
        find_queues(msg.routing_key, headers, queues, exchanges)
        if queues.empty? || (immediate && !queues.any? &.immediate_delivery?)
          @unroutable_count.add(1, :relaxed)
          return false
        end

        count = 0u32
        queues.each do |queue|
          if queue.publish(msg)
            count += 1
            msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
          end
        end
        @publish_out_count.add(count, :relaxed)
        @unroutable_count.add(1, :relaxed) if count.zero?
        count.positive?
      end

      def find_queues(routing_key : String, headers : AMQP::Table?,
                      queues : Set(LavinMQ::Queue) = Set(LavinMQ::Queue).new,
                      exchanges : Set(LavinMQ::Exchange) = Set(LavinMQ::Exchange).new) : Nil
        return unless exchanges.add? self
        each_destination(routing_key, headers) do |d|
          case d
          in LavinMQ::Queue
            queues.add(d)
          in LavinMQ::Exchange
            d.find_queues(routing_key, headers, queues, exchanges)
          end
        end

        if hdrs = headers
          find_cc_queues(hdrs, "CC", queues)
          find_cc_queues(hdrs, "BCC", queues)
          hdrs.delete "BCC"
        end

        if queues.empty? && alternate_exchange
          @vhost.exchanges[alternate_exchange]?.try do |ae|
            ae.find_queues(routing_key, headers, queues, exchanges)
          end
        end
      end

      private def find_cc_queues(headers, key, queues)
        return unless cc = headers[key]?
        cc = cc.as?(Array(AMQP::Field))

        raise LavinMQ::Error::PreconditionFailed.new("#{key} header not a string array") unless cc

        hdrs = headers.clone
        hdrs.delete "CC"
        hdrs.delete key
        cc.each do |rk|
          if rk = rk.as?(String)
            find_queues(rk, hdrs, queues)
          else
            raise LavinMQ::Error::PreconditionFailed.new("#{key} header not a string array")
          end
        end
      end

      private def should_delay_message?(headers)
        return false if headers.nil? || headers.empty?
        return false unless delayed?
        x_delay = headers["x-delay"]?
        return false unless x_delay
        x_deaths = headers["x-death"]?.try(&.as?(Array(AMQP::Field)))
        x_death = x_deaths.try(&.first).try(&.as?(AMQP::Table))
        x_death.nil? || (x_death["queue"]? != "amq.delayed.#{@name}")
      end

      def to_json(json : JSON::Builder)
        json.object do
          details_tuple.merge(message_stats: stats_details).each do |k, v|
            json.field(k, v) unless v.nil?
          end
        end
      end
    end
  end
end
