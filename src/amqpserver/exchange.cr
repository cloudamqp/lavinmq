module AMQPServer
  class Exchange
    getter name, type, durable, bindings, arguments

    def initialize(@name : String, @type : String, @durable : Bool,
                   @arguments : Hash(String, AMQP::Field),
                   @bindings = Hash(String, Array(Queue)).new)
    end

    def to_json(builder : JSON::Builder)
      { name: @name, type: @type, durable: @durable, arguments: @arguments, }.to_json(builder)
    end

    def queues_matching(routing_key) : Array(Queue)
      case @type
      when "direct"
        @bindings.fetch(routing_key, Array(Queue).new)
      when "fanout"
        @bindings.values.flatten
      when "topic"
        @bindings.select do |binding_key, queues|
          next true if routing_key == binding_key
          rk_parts = binding_key.split(".")
          routing_key.split(".").each_with_index do |part|
          end
        end.values.flatten
      else raise "Exchange type #{@type} not implemented"
      end
    end

    class RKMather
      def self.topic(rk, binding_keys)
        rk_parts = rk.split(".")
        binding_keys.select do |bk|
          ok = false
          bk.split(".").each_with_index do |part, i|
            if rk_parts.size < i + 1
              ok = false
              break
            end
            ok = true if part == "*" || part == rk_parts[i]
          end
          ok
        end
      end
    end
  end
end
