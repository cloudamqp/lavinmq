module AvalancheMQ
  abstract class Exchange
    getter name, durable, auto_delete, internal, arguments, queue_bindings, exchange_bindings
    def_equals_and_hash @vhost.name, @name

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = AMQP::Table.new)
      @queue_bindings = Hash(Tuple(String, AMQP::Table),
                             Set(String)).new { |h, k| h[k] = Set(String).new }
      @exchange_bindings = Hash(Tuple(String, AMQP::Table),
                                Set(String)).new { |h, k| h[k] = Set(String).new }
    end

    def to_json(builder : JSON::Builder)
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        queue_bindings: @queue_bindings, exchange_bindings: @exchange_bindings
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

    def queues_matching(routing_key : String, headers : AMQP::Table? = nil)
      matches(@queue_bindings, routing_key, headers)
    end

    def exchanges_matching(routing_key : String, headers : AMQP::Table? = nil)
      matches(@exchange_bindings, routing_key, headers)
    end

    def bind_queue(queue : String, routing_key : String, headers : AMQP::Table? = nil) : Nil
      bind(@queue_bindings, queue, routing_key, headers)
    end

    def unbind_queue(queue : String, routing_key : String, headers : AMQP::Table? = nil) : Nil
      unbind(@queue_bindings, queue, routing_key, headers)
    end

    def bind_exchange(exchange : String, routing_key : String, headers : AMQP::Table? = nil) : Nil
      bind(@exchange_bindings, exchange, routing_key, headers)
    end

    def unbind_exchange(exchange : String, routing_key : String, headers : AMQP::Table? = nil) : Nil
      unbind(@exchange_bindings, exchange, routing_key, headers)
    end

    abstract def type : String
    abstract def bind(bindings, destination : String, routing_key : String, headers : AMQP::Table) : Nil
    abstract def unbind(bindings, destination : String, routing_key : String, headers : AMQP::Table) : Nil
    abstract def matches(bindings, routing_key : String, headers : AMQP::Table) : Set(String)
  end

  class DirectExchange < Exchange
    def type
      "direct"
    end

    def bind(bindings, destination, routing_key, headers)
      bindings[{routing_key, AMQP::Table.new}] << destination
    end

    def unbind(bindings, destination, routing_key, headers)
      bindings[{routing_key, AMQP::Table.new}].delete destination
    end

    def matches(bindings, routing_key, headers)
      bindings[{routing_key, AMQP::Table.new}]
    end
  end

  class FanoutExchange < Exchange
    def type
      "fanout"
    end

    def bind(bindings, destination, routing_key, headers)
      bindings[{"", AMQP::Table.new}] << destination
    end

    def unbind(bindings, destination, routing_key, headers)
      bindings[{"", AMQP::Table.new}].delete destination
    end

    def matches(bindings, routing_key, headers)
      bindings[{"", AMQP::Table.new}]
    end
  end

  class TopicExchange < Exchange
    def type
      "topic"
    end

    def bind(bindings, destination, routing_key, headers)
      bindings[{routing_key, AMQP::Table.new}] << destination
    end

    def unbind(bindings, destination, routing_key, headers)
      bindings[{routing_key, AMQP::Table.new}].delete destination
    end

    def matches(bindings, routing_key, headers) : Set(String)
      rk_parts = routing_key.split(".")
      s = Set(String).new
      bindings.each do |bt, q|
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

    def bind(bindings, destination, routing_key, headers)
      unless (headers.has_key?("x-match") && headers.size >= 2) || headers.size == 1
        raise ArgumentError.new("Arguments required")
      end
      bindings[{"", headers}] << destination
    end

    def unbind(bindings, destination, routing_key, headers)
      bindings[{"", headers}].delete destination
    end

    def matches(bindings, routing_key, headers) : Set(String)
      matches = Set(String).new
      return matches unless headers
      bindings.each do |bt, queues|
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
