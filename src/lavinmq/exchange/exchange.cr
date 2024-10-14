require "../policy"
require "../stats"
require "../amqp"
require "../sortable_json"
require "../observable"
require "./event"
require "../queue"

module LavinMQ
  alias Destination = Queue | Exchange
  record BindingKey, routing_key : String, arguments : AMQP::Table? = nil do
    def properties_key
      if arguments.nil? || arguments.try(&.empty?)
        routing_key.empty? ? "~" : routing_key
      else
        hsh = Base64.urlsafe_encode(arguments.to_s)
        "#{routing_key}~#{hsh}"
      end
    end
  end

  abstract class Exchange
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

    rate_stats({"publish_in", "publish_out", "unroutable"})
    property publish_in_count, publish_out_count, unroutable_count

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
          @vhost.upstreams.try &.link(v.as_s, self)
        when "federation-upstream-set"
          @vhost.upstreams.try &.link_set(v.as_s, self)
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
    end

    def handle_arguments
      @alternate_exchange = (@arguments["x-alternate-exchange"]? || @arguments["alternate-exchange"]?).try &.to_s
      if @arguments["x-delayed-exchange"]?.try &.as?(Bool)
        @delayed = true
        init_delayed_queue
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
        x.bindings_details.each.any? { |bd| bd.destination == self }
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
                         DurableDelayedExchangeQueue.new(@vhost, q_name, false, false, arguments)
                       else
                         DelayedExchangeQueue.new(@vhost, q_name, false, false, arguments)
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

    abstract def type : String
    abstract def bind(destination : Destination, routing_key : String, headers : AMQP::Table?)
    abstract def unbind(destination : Destination, routing_key : String, headers : AMQP::Table?)
    abstract def bindings_details : Iterator(BindingDetails)

    def publish(msg : Message, immediate : Bool,
                queues : Set(Queue) = Set(Queue).new,
                exchanges : Set(Exchange) = Set(Exchange).new) : Int32
      @publish_in_count += 1
      count = do_publish(msg, immediate, queues, exchanges)
      @unroutable_count += 1 if count.zero?
      @publish_out_count += count
      count
    end

    def do_publish(msg : Message, immediate : Bool,
                   queues : Set(Queue) = Set(Queue).new,
                   exchanges : Set(Exchange) = Set(Exchange).new) : Int32
      headers = msg.properties.headers
      if should_delay_message?(headers)
        if q = @delayed_queue
          q.publish(msg)
          return 1
        else
          return 0
        end
      end
      find_queues(msg.routing_key, headers, queues, exchanges)
      return 0 if queues.empty?
      return 0 if immediate && !queues.any? &.immediate_delivery?

      count = 0
      queues.each do |queue|
        if queue.publish(msg)
          count += 1
          msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
        end
      end
      count
    end

    def find_queues(routing_key : String, headers : AMQP::Table?,
                    queues : Set(Queue),
                    exchanges : Set(Exchange) = Set(Exchange).new) : Nil
      return unless exchanges.add? self
      bindings(routing_key, headers).each do |d|
        case d
        when Queue
          queues.add(d)
        when Exchange
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

      raise Error::PreconditionFailed.new("#{key} header not a string array") unless cc

      hdrs = headers.clone
      hdrs.delete "CC"
      hdrs.delete key
      cc.each do |rk|
        if rk = rk.as?(String)
          find_queues(rk, hdrs, queues)
        else
          raise Error::PreconditionFailed.new("#{key} header not a string array")
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

    class AccessRefused < Error
      def initialize(@exchange : Exchange)
        super("Access refused to #{@exchange.name}")
      end
    end
  end

  struct BindingDetails
    include SortableJSON
    getter source, vhost, binding_key, destination

    def initialize(@source : String, @vhost : String,
                   @binding_key : BindingKey, @destination : Destination)
    end

    def arguments
      @binding_key.arguments
    end

    def routing_key
      @binding_key.routing_key
    end

    def details_tuple
      {
        source:           @source,
        vhost:            @vhost,
        destination:      @destination.name,
        destination_type: @destination.is_a?(Queue) ? "queue" : "exchange",
        routing_key:      @binding_key.routing_key,
        arguments:        @binding_key.arguments,
        properties_key:   @binding_key.properties_key,
      }
    end
  end
end
