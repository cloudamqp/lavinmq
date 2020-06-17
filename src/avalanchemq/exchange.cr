require "logger"
require "./policy"
require "./stats"
require "./amqp"
require "./queue"
require "./persistent_exchange_queue"
require "./sortable_json"

module AvalancheMQ
  alias BindingKey = Tuple(String, Hash(String, AMQP::Field)?)
  alias Destination = Queue | Exchange

  abstract class Exchange
    include PolicyTarget
    include Stats
    include SortableJSON

    getter name, durable, auto_delete, internal, arguments, queue_bindings, exchange_bindings, vhost, type, alternate_exchange
    getter policy : Policy?

    @alternate_exchange : String?
    @persist_messages : ArgumentNumber?
    @persistent_queue : PersistentExchangeQueue?
    @log : Logger

    rate_stats(%w(publish_in publish_out))
    property publish_in_count, publish_out_count

    def initialize(@vhost : VHost, @name : String, @durable = false,
                   @auto_delete = false, @internal = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @queue_bindings = Hash(BindingKey, Set(Queue)).new do |h, k|
        h[k] = Set(Queue).new
      end
      @exchange_bindings = Hash(BindingKey, Set(Exchange)).new do |h, k|
        h[k] = Set(Exchange).new
      end
      @log = @vhost.log.dup
      @log.progname += " exchange=#{@name}"
      handle_arguments
    end

    def apply_policy(policy : Policy)
      handle_arguments
      policy.not_nil!.definition.each do |k, v|
        @log.debug { "Applying policy #{k}: #{v}" }
        case k
        when "alternate-exchange"
          @alternate_exchange = v.as_s?
        else nil
        end
      end
      @policy = policy
    end

    def clear_policy
      handle_arguments
      @policy = nil
    end

    def handle_arguments
      @alternate_exchange = @arguments["x-alternate-exchange"]?.try &.to_s
      @persist_messages = @arguments["x-persist-messages"]?.try &.as?(ArgumentNumber)
      @persist_messages.try do |pm|
        if  pm > 0
          q_name = "amq.persistent.#{@name}"
          unless @vhost.queues.has_key? q_name
            @persistent_queue = PersistentExchangeQueue.new(@vhost, q_name)
            @vhost.queues[q_name] =  @persistent_queue.not_nil!
          end
        end
      end
    end

    def details_tuple
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        policy: @policy.try &.name, effective_policy_definition: @policy,
        message_stats: stats_details,
      }
    end

    def self.make(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "fanout"
        FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      else raise "Cannot make exchange type #{type}"
      end
    end

    def match?(frame : AMQP::Frame)
      type == frame.exchange_type &&
        @durable == frame.durable &&
        @auto_delete == frame.auto_delete &&
        @internal == frame.internal &&
        @arguments == frame.arguments.to_h
    end

    def match?(type, durable, auto_delete, internal, arguments)
      self.type == type &&
        @durable == durable &&
        @auto_delete == auto_delete &&
        @internal == internal &&
        @arguments == arguments.to_h
    end

    def in_use?
      return true if @queue_bindings.size > 0
      return true if @exchange_bindings.size > 0
      return true if @vhost.exchanges.any? { |_, x| x.exchange_bindings.any? { |_, exs| exs.includes? self } }
      false
    end

    def bindings_details
      arr = Array(BindingDetails).new(@queue_bindings.size + @exchange_bindings.size)
      @queue_bindings.each do |key, desinations|
        desinations.each { |destination| arr << binding_details(key, destination) }
      end
      @exchange_bindings.each do |key, desinations|
        desinations.each { |destination| arr << binding_details(key, destination) }
      end
      arr
    end

    def binding_details(key, destination)
      BindingDetails.new(name, vhost.name, key, destination)
    end

    def persistent?
      !@persistent_queue.nil?
    end

    private def after_bind(destination : Destination, headers : Hash(String, AMQP::Field)?)
      return true if !persistent? || headers.nil? || headers.not_nil!.empty? || @persistent_queue.not_nil!.empty?
      if head = headers.not_nil!["x-head"]?.try &.as?(ArgumentNumber)
        pq = @persistent_queue.not_nil!
        persisted = pq.message_count
        @log.debug { "after_bind replaying persited message from head-#{head}, total_peristed: #{persisted}" }
        case destination
        when Queue
          pq.peek(head) do |sp|
            # next if destination.includes_segment_position?(sp)
            next unless destination.publish(sp)
            # publish_out_count += 1
            @vhost.sp_counter.inc(sp)
          end
        when Exchange
          # TODO @vhost.publish_segment_position(sp, self, Set(Queue).new([destination]))
        end
      end
      true
    end

    private def after_unbind
      if @auto_delete &&
         @queue_bindings.each_value.all? &.empty? &&
         @exchange_bindings.each_value.all? &.empty?
        delete
      end
    end

    protected def delete
      @log.info { "Deleting exchange: #{@name}" }
      @vhost.apply AMQP::Frame::Exchange::Delete.new 0_u16, 0_u16, @name, false, false
    end

    abstract def type : String
    abstract def bind(destination : Queue, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def unbind(destination : Queue, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def bind(destination : Exchange, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def unbind(destination : Exchange, routing_key : String, headers : Hash(String, AMQP::Field)?)
    abstract def queue_matches(routing_key : String, headers : Hash(String, AMQP::Field)?, &blk : Queue -> _)
    abstract def exchange_matches(routing_key : String, headers : Hash(String, AMQP::Field)?, &blk : Exchange -> _)
  end

  struct BindingDetails
    include SortableJSON
    getter source, vhost, key, destination

    def initialize(@source : String, @vhost : String,
                   @key : BindingKey, @destination : Queue | Exchange)
    end

    def details_tuple
      {
        source:           @source,
        vhost:            @vhost,
        destination:      @destination.name,
        destination_type: @destination.is_a?(Queue) ? "queue" : "exchange",
        routing_key:      @key[0],
        arguments:        @key[1],
        properties_key:   BindingDetails.hash_key(@key),
      }
    end

    def self.hash_key(key : BindingKey)
      if key[1].nil? || key[1].try &.empty?
        key[0]
      else
        hsh = Base64.urlsafe_encode(key[1].to_s)
        "#{key[0]}~#{hsh}"
      end
    end
  end

  class DirectExchange < Exchange
    def type : String
      "direct"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination
      after_bind(destination, headers)
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}] << destination
      after_bind(destination, headers)
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}].delete destination
      after_unbind
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}].delete destination
      after_unbind
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      @queue_bindings[{routing_key, nil}].each { |q| yield q }
      @persistent_queue.try { |q| yield q } if persistent?
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      @exchange_bindings[{routing_key, nil}].each { |x| yield x }
    end
  end

  class DefaultExchange < Exchange
    def type : String
      "direct"
    end

    def bind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def unbind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      if q = @vhost.queues[routing_key]?
        yield q
      end
      @persistent_queue.try { |q| yield q } if persistent?
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      # noop
    end
  end

  class FanoutExchange < Exchange
    def type : String
      "fanout"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination
      after_bind(destination, headers)
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}] << destination
      after_bind(destination, headers)
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}].delete destination
      after_unbind
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}].delete destination
      after_unbind
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      @queue_bindings.each_value { |s| s.each { |q| yield q } }
      @persistent_queue.try { |q| yield q } if persistent?
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      @exchange_bindings.each_value { |s| s.each { |q| yield q } }
    end
  end

  class TopicExchange < Exchange
    def initialize(*args)
      super(*args)
      @queue_binding_keys = Hash(Array(String), Set(Queue)).new do |h, k|
        h[k] = Set(Queue).new
      end
      @exchange_binding_keys = Hash(Array(String), Set(Exchange)).new do |h, k|
        h[k] = Set(Exchange).new
      end
    end

    def type : String
      "topic"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination
      @queue_binding_keys[routing_key.split(".")] << destination
      after_bind(destination, headers)
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}] << destination
      @exchange_binding_keys[routing_key.split(".")] << destination
      after_bind(destination, headers)
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}].delete destination
      @queue_binding_keys[routing_key.split(".")].delete destination
      after_unbind
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}].delete destination
      @exchange_binding_keys[routing_key.split(".")].delete destination
      after_unbind
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      matches(@queue_binding_keys, routing_key, headers) { |q| yield q.as(Queue) }
      @persistent_queue.try { |q| yield q } if persistent?
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      matches(@exchange_binding_keys, routing_key, headers) { |e| yield e.as(Exchange) }
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def matches(binding_keys, routing_key, headers = nil, &blk : Queue | Exchange -> _)
      rk_parts = routing_key.split(".")
      binding_keys.each do |bks, dst|
        ok = false
        prev_hash = false
        size = bks.size # binding keys can max be 256 chars long anyway
        j = 0
        i = 0
        bks.each do |part|
          if rk_parts.size <= j
            ok = false
            break
          end
          case part
          when "#"
            j += 1
            prev_hash = true
            ok = true
          when "*"
            prev_hash = false
            # Is this the last bk and the last rk?
            if size == i + 1 && rk_parts.size == j + 1
              ok = true
              break
              # More than 1 rk left ok move on
            elsif rk_parts.size > j + 1
              j += 1
            else
              ok = false
              j += 1
            end
          else
            if prev_hash
              if size == (i + 1)
                ok = rk_parts.last == part
                j += 1
              else
                ok = false
                rk_parts[j..-1].each do |rk_part|
                  j += 1
                  ok = part == rk_part
                  break if ok
                end
              end
            else
              # Is this the last bk but not the last rk?
              if size == i + 1 && rk_parts.size > j + 1
                ok = false
              else
                ok = rk_parts[j] == part
              end
              j += 1
            end
          end
          break unless ok
          i += 1
        end
        dst.each { |d| yield d } if ok
      end
    end
  end

  class HeadersExchange < Exchange
    def type : String
      "headers"
    end

    def bind(destination : Queue, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @queue_bindings[{routing_key, args}] << destination
      after_bind(destination, headers)
    end

    def bind(destination : Exchange, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @exchange_bindings[{routing_key, args}] << destination
      after_bind(destination, headers)
    end

    def unbind(destination : Queue, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @queue_bindings[{routing_key, args}].delete destination
      after_unbind
    end

    def unbind(destination : Exchange, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @exchange_bindings[{routing_key, args}].delete destination
      after_unbind
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue ->)
      matches(@queue_bindings, routing_key, headers) { |d| yield d.as(Queue) }
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange ->)
      matches(@exchange_bindings, routing_key, headers) { |d| yield d.as(Exchange) }
    end

    private def matches(bindings, routing_key, headers, &blk : Queue | Exchange ->)
      bindings.each do |bt, dst|
        args = bt[1] || next
        if headers.nil? || headers.empty?
          if args.empty?
            dst.each { |d| yield d }
          end
        else
          case args["x-match"]?
          when "any"
            if args.any? { |k, v| k != "x-match" && headers[k]? == v }
              dst.each { |d| yield d }
            end
          else
            if args.all? { |k, v| k == "x-match" || headers[k]? == v }
              dst.each { |d| yield d }
            end
          end
        end
      end
    end
  end
end
