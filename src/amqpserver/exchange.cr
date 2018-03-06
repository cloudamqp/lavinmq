module AMQPServer
  abstract class Exchange
    getter name, type, durable, auto_delete, internal, bindings, arguments

    def initialize(@vhost : VHost, @name : String, @type : String, @durable : Bool,
                   @auto_delete : Bool, @internal : Bool,
                   @arguments = Hash(String, AMQP::Field).new)
      @bindings = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
    end

    def to_json(builder : JSON::Builder)
      {
        name: @name, type: @type, durable: @durable, auto_delete: @auto_delete,
        internal: @internal, arguments: @arguments, vhost: @vhost.name,
        bindings: @bindings
      }.to_json(builder)
    end

    def self.make(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        if name.empty?
          DefaultExchange.new vhost
        else
          DirectExchange.new(vhost, name, type, durable, auto_delete, internal, arguments)
        end
      when "fanout"
        FanoutExchange.new(vhost, name, type, durable, auto_delete, internal, arguments)
      when "topic"
        TopicExchange.new(vhost, name, type, durable, auto_delete, internal, arguments)
      else raise "Cannot make exchange type #{type}"
      end
    end

    abstract def queues_matching(routing_key : String) : Array(String)
    abstract def bind(queue : String, routing_key : String,
                      arguments : Hash(String, AMQP::Field))
    abstract def unbind(queue : String, routing_key : String)
  end

  class DirectExchange < Exchange
    def bind(queue_name, routing_key, arguments)
      @bindings[routing_key] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[routing_key].delete queue_name
    end

    def queues_matching(routing_key)
      @bindings[routing_key]
    end
  end

  class DefaultExchange < DirectExchange
    def initialize(vhost)
      super(vhost, "", type: "direct", durable: true, auto_delete: false, internal: true, arguments: Hash(String, AMQP::Field).new)
    end

    def queues_matching(routing_key)
      @bindings[routing_key] & Set.new([routing_key])
    end
  end

  class FanoutExchange < Exchange
    def bind(queue_name, routing_key, arguments)
      @bindings[""] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[""].delete queue_name
    end

    def queues_matching(routing_key)
      @bindings[""]
    end
  end

  class TopicExchange < Exchange
    def bind(queue_name, routing_key, arguments)
      @bindings[routing_key] << queue_name
    end

    def unbind(queue_name, routing_key)
      @bindings[routing_key].delete queue_name
    end

    def queues_matching(routing_key)
      rk_parts = routing_key.split(".")
      @bindings.select do |bk|
        ok = false
        bk.split(".").each_with_index do |part, i|
          if rk_parts.size < i + 1
            ok = false
            break
          end
          ok = true if part == "*" || part == rk_parts[i]
        end
        ok
      end.values.flatten
    end
  end
end
