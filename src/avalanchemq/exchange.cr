require "logger"
require "./policy"
require "./stats"
require "./amqp"
require "./queue"
require "./sortable_json"

module AvalancheMQ
  alias BindingKey = Tuple(String, Hash(String, AMQP::Field)?)
  alias Destination = Set(Queue | Exchange)

  abstract class Exchange
    include PolicyTarget
    include Stats
    include SortableJSON

    getter name, durable, auto_delete, internal, arguments, queue_bindings, exchange_bindings, vhost, type, alternate_exchange
    getter policy : Policy?

    @alternate_exchange : String?
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
      @queue_bindings.size > 0 || @exchange_bindings.size > 0
    end

    def bindings_details
      @bindings.flat_map do |key, desinations|
        desinations.map { |destination| binding_details(key, destination) }
      end
    end

    def binding_details(key, destination)
      BindingDetails.new(name, vhost.name, key, destination)
    end

    private def after_unbind
      if @auto_delete && @bindings.each_value.none? { |s| s.size > 0 }
        delete
      end
    end

    protected def delete
      @log.info { "Deleting exchange: #{@name}" }
      @vhost.apply AMQP::Frame::Exchange::Delete.new 0_u16, 0_u16, @name, false, false
    end

    abstract def type : String
    abstract def bind(destination : Queue | Exchange, routing_key : String,
                      headers : Hash(String, AMQP::Field)?)
    abstract def unbind(destination : Queue | Exchange, routing_key : String,
                        headers : Hash(String, AMQP::Field)?)
    abstract def matches(routing_key : String, headers : Hash(String, AMQP::Field)?) : Set(Queue | Exchange)
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

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}].delete destination
      after_unbind
    end

    def matches(routing_key, headers = nil) : Set(Queue | Exchange)
      @bindings[{routing_key, nil}]
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      @queue_bindings[{routing_key, nil}].each
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      @exchange_bindings[{routing_key, nil}].each
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

    def matches(routing_key, headers = nil) : Set(Queue | Exchange)
      Set(Queue | Exchange).new(@vhost.queues[routing_key])
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      if q = @vhost.queues[routing_key]?
        yield q
      end
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
    end
  end

  class FanoutExchange < Exchange
    def initialize(*args)
      super(*args)
      @destinations = Set(Queue | Exchange).new
    end

    def type : String
      "fanout"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}] << destination
      @destinations << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}].delete destination
      @destinations.delete destination
      after_unbind
    end

    def matches(routing_key, headers = nil) : Set(Queue | Exchange)
      @destinations
    end

    def queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      @queue_bindings.each
    end

    def exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      @exchange_bindings.each
    end
  end

  class TopicExchange < Exchange
    def initialize(*args)
      super(*args)
      @binding_keys = Hash(Array(String), Destination).new do |h, k|
        h[k] = Set(Queue | Exchange).new
      end
    end

    def type : String
      "topic"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}] << destination
      @binding_keys[routing_key.split(".")] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, nil}].delete destination
      @binding_keys[routing_key.split(".")].delete destination
      after_unbind
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def matches(routing_key, headers = nil) : Set(Queue | Exchange)
      rk_parts = routing_key.split(".")
      s = Set(Queue | Exchange).new
      @binding_keys.each do |bks, dst|
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
        s.concat(dst) if ok
      end
      s
    end
  end

  class HeadersExchange < Exchange
    def type : String
      "headers"
    end

    def bind(destination, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @bindings[{routing_key, args}] << destination
    end

    def unbind(destination, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @bindings[{routing_key, args}].delete destination
      after_unbind
    end

    def matches(routing_key, headers) : Set(Queue | Exchange)
      matches = Set(Queue | Exchange).new
      return matches unless headers
      @bindings.each do |bt, queues|
        args = bt[1]
        next unless args
        case args["x-match"]?
        when "any"
          if args.any? { |k, v| k != "x-match" && headers[k]? == v }
            matches.concat(queues)
          end
        else
          if args.all? { |k, v| k == "x-match" || headers[k]? == v }
            matches.concat(queues)
          end
        end
      end
      matches
    end
  end
end
