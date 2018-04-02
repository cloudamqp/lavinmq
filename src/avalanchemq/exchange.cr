module AvalancheMQ
  abstract class Exchange
    getter name, durable, auto_delete, internal, arguments, bindings
    def_equals_and_hash @vhost.name, @name

    def initialize(@vhost : VHost, @name : String, @durable = false,
                   @auto_delete = false, @internal = false,
                   @arguments = AMQP::Table.new)
      @bindings = Hash(Tuple(String, AMQP::Table), Set(Queue | Exchange)).new do |h, k|
        h[k] = Set(Queue | Exchange).new
      end
    end

    def to_json(builder : JSON::Builder)
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        bindings: @bindings
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

    abstract def type : String
    abstract def bind(destination : Queue | Exchange, routing_key : String,
                      headers : AMQP::Table?) : Nil
    abstract def unbind(destination : Queue | Exchange, routing_key : String,
                        headers : AMQP::Table?) : Nil
    abstract def matches(routing_key : String, headers : AMQP::Table?) : Set(Queue | Exchange)
  end

  class DirectExchange < Exchange
    def type
      "direct"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, AMQP::Table.new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, AMQP::Table.new}].delete destination
    end

    def matches(routing_key, headers = nil)
      @bindings[{routing_key, AMQP::Table.new}]
    end
  end

  class FanoutExchange < Exchange
    def type
      "fanout"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{"", AMQP::Table.new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{"", AMQP::Table.new}].delete destination
    end

    def matches(routing_key, headers = nil)
      @bindings[{"", AMQP::Table.new}]
    end
  end

  class TopicExchange < Exchange
    def type
      "topic"
    end

    def bind(destination, routing_key, headers = nil)
      @bindings[{routing_key, AMQP::Table.new}] << destination
    end

    def unbind(destination, routing_key, headers = nil)
      @bindings[{routing_key, AMQP::Table.new}].delete destination
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
