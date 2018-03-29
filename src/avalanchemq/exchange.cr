module AvalancheMQ
  abstract class Exchange
    getter name, durable, auto_delete, internal, arguments

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
    end

    def to_json(builder : JSON::Builder)
      {
        name: @name, type: type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        bindings: bindings
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
    abstract def queues_matching(routing_key : String, headers : Hash(String, AMQP::Field)) : Set(String)
    abstract def bind(queue : String, routing_key : String, arguments : Hash(String, AMQP::Field))
    abstract def unbind(queue : String, routing_key : String)
  end

  class DirectExchange < Exchange
    getter bindings

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      super
    end

    def type
      "direct"
    end

    def bind(queue_name, routing_key, headers = nil)
      @bindings[routing_key] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[routing_key].delete queue_name
    end

    def queues_matching(routing_key, headers = nil)
      @bindings[routing_key]
    end
  end

  class FanoutExchange < Exchange
    getter bindings

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      super
    end

    def type
      "fanout"
    end

    def bind(queue_name, routing_key, headers = nil)
      @bindings[""] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[""].delete queue_name
    end

    def queues_matching(routing_key, headers = nil)
      @bindings[""]
    end
  end

  class TopicExchange < Exchange
    getter bindings

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      super
    end

    def type
      "topic"
    end

    def bind(queue_name, routing_key, headers = nil)
      @bindings[routing_key] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[routing_key].delete queue_name
    end

    def queues_matching(routing_key, headers = nil) : Set(String)
      rk_parts = routing_key.split(".")
      s = Set(String).new
      @bindings.each do |bk, q|
        ok = false
        bk_parts = bk.split(".")
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
    getter bindings

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(String, Set(Hash(String, AMQP::Field)))
        .new { |h, k| h[k] = Set(Hash(String, AMQP::Field)).new }
      super
    end

    def type
      "headers"
    end

    def bind(queue_name, routing_key, headers = Hash(String, AMQP::Field).new)
      args = @arguments.merge(headers)
      unless (args.has_key?("x-match") && args.size >= 2) || args.size == 1
        raise ArgumentError.new("Arguments required")
      end
      @bindings[queue_name] << args
      @vhost.log.debug("Binding #{queue_name} with #{args}")
    end

    def unbind(queue_name, routing_key)
      @bindings.delete queue_name
    end

    def queues_matching(routing_key, headers = nil) : Set(String)
      matches = Set(String).new
      return matches unless headers
      @bindings.each do |queue_name, arguments_set|
        arguments_set.each do |args|
          case args["x-match"]
          when "any"
            if headers.any? { |k, v| k != "x-match" && args.has_key?(k) && args[k] == v }
              matches << queue_name
            end
          else
            if headers.all? { |k, v| args.has_key?(k) && args[k] == v }
              matches << queue_name
            end
          end
        end
      end
      matches
    end
  end
end
