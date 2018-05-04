require "base64"
require "./policy"

module AvalancheMQ
  abstract class Exchange
    include PolicyTarget

    getter name, durable, auto_delete, internal, arguments, bindings, policy, vhost, type
    def_equals_and_hash @vhost.name, @name

    def initialize(@vhost : VHost, @name : String, @durable = false,
                   @auto_delete = false, @internal = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(Tuple(String, Hash(String, AMQP::Field)), Set(Queue | Exchange)).new do |h, k|
        h[k] = Set(Queue | Exchange).new
      end
    end

    def apply_policy(@policy : Policy)
      @policy.not_nil!.definition.each do |k, v|
        case k
        when "alternate-exchange"
          # TODO
        end
      end
    end

    def to_json(builder : JSON::Builder)
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        policy: @policy
      }.to_json(builder)
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
        @arguments == frame.arguments
    end

    def match?(type, durable, auto_delete, internal, arguments)
      self.type == type &&
        @durable == durable &&
        @auto_delete == auto_delete &&
        @internal == internal &&
        @arguments == arguments
    end

    def bindings_details
      @bindings.flat_map do |key, desinations|
        desinations.map do |destination|
          {
            source: name,
            vhost: vhost.name,
            destination: destination.name,
            destination_type: destination.class.name.downcase,
            routing_key: key[0],
            arguments: key[1],
            properties_key: hash_key(key)
          }
        end
      end
    end

    def binding_details(destination : Queue | Exchange, properties_key : String)
      bindings_details.find { |b| b[:properties_key] == properties_key }
    end

    def unbind(destination : Queue | Exchange, properties_key : String)
      key = parse_key(properties_key)
      unbind(destination, key[0], key[1]) if key
    end

    private def parse_key(properties_key : String)
      @bindings.keys.find { |k| hash_key(k) == properties_key }
    end

    private def hash_key(key : Tuple(String, Hash(String, AMQP::Field)))
      hsh = Base64.encode(key[1].to_s)
      "#{key[0]}~#{hsh}"
    end

    private def after_unbind
      if @auto_delete && @bindings.values.none? { |s| s.size > 0 }
        delete
      end
    end

    protected def delete
      @vhost.log.info "Deleting exchange: #{@name}"
      @vhost.apply AMQP::Exchange::Delete.new 0_u16, 0_u16, @name, false, false
    end

    abstract def type : String
    abstract def bind(destination : Queue | Exchange, routing_key : String,
                      headers : Hash(String, AMQP::Field)?) : Nil
    abstract def unbind(destination : Queue | Exchange, routing_key : String,
                        headers : Hash(String, AMQP::Field)?) : Nil
    abstract def matches(routing_key : String, headers : Hash(String, AMQP::Field)?) : Set(Queue | Exchange)
  end

  class DirectExchange < Exchange
    def type
      "direct"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, Hash(String, AMQP::Field).new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, Hash(String, AMQP::Field).new}].delete destination
      after_unbind
    end

    def matches(routing_key, headers = nil)
      @bindings[{routing_key, Hash(String, AMQP::Field).new}]
    end
  end

  class FanoutExchange < Exchange
    def type
      "fanout"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{"", Hash(String, AMQP::Field).new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{"", Hash(String, AMQP::Field).new}].delete destination
      after_unbind
    end

    def matches(routing_key, headers = nil)
      @bindings[{"", Hash(String, AMQP::Field).new}]
    end
  end

  class TopicExchange < Exchange
    def type
      "topic"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, Hash(String, AMQP::Field).new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, Hash(String, AMQP::Field).new}].delete destination
      after_unbind
    end

    def matches(routing_key, headers = nil)
      rk_parts = routing_key.split(".")
      s = Set(Queue | Exchange).new
      @bindings.each do |bt, q|
        ok = false
        bk_parts = bt[0].split(".")
        bk_parts.each_with_index do |part, i|
          if part == "#"
            ok = true
            break
          end
          if part == "*" || part == rk_parts[i]
            if bk_parts.size == i + 1 && rk_parts.size > i + 1
              ok = false
            else
              ok = true
            end
            next
          else
            ok = false
            break
          end
        end
        s.concat(q) if ok
      end
      s
    end
  end

  class HeadersExchange < Exchange
    def type
      "headers"
    end

    def bind(destination, routing_key, headers)
      args = @arguments.merge(headers)
      unless (args.has_key?("x-match") && args.size >= 2) || args.size == 1
        raise ArgumentError.new("Arguments required")
      end
      @bindings[{"", args}] << destination
    end

    def unbind(destination, routing_key, headers)
      args = @arguments.merge(headers)
      @bindings[{"", args}].delete destination
      after_unbind
    end

    def matches(routing_key, headers)
      matches = Set(Queue | Exchange).new
      return matches unless headers
      @bindings.each do |bt, queues|
        args = bt[1]
        case args["x-match"]
        when "any"
          if headers.any? { |k, v| k != "x-match" && args.has_key?(k) && args[k] == v }
            matches.concat(queues)
          end
        else
          if headers.all? { |k, v| args.has_key?(k) && args[k] == v }
            matches.concat(queues)
          end
        end
      end
      matches
    end
  end
end
