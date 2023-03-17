require "./exchange"

module LavinMQ
  class FanoutExchange < Exchange
    def type : String
      "fanout"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination
      after_bind(destination, routing_key, headers)
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}] << destination
      after_bind(destination, routing_key, headers)
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}].delete destination
      after_unbind(destination, routing_key, headers)
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      @exchange_bindings[{routing_key, nil}].delete destination
      after_unbind(destination, routing_key, headers)
    end

    def do_queue_matches(routing_key, headers = nil, & : Queue -> _)
      @queue_bindings.each_value { |s| s.each { |q| yield q } }
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      @exchange_bindings.each_value { |s| s.each { |q| yield q } }
    end
  end
end
