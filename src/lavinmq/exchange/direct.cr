require "./exchange"

module LavinMQ
  class DirectExchange < Exchange
    def type : String
      "direct"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      ret = @queue_bindings[{routing_key, nil}].add? destination
      after_bind(destination, routing_key, headers)
      ret
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      ret = @exchange_bindings[{routing_key, nil}].add? destination
      after_bind(destination, routing_key, headers)
      ret
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      ret = @queue_bindings[{routing_key, nil}].delete destination
      after_unbind(destination, routing_key, headers)
      ret
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      ret = @exchange_bindings[{routing_key, nil}].delete destination
      after_unbind(destination, routing_key, headers)
      ret
    end

    def do_queue_matches(routing_key, headers = nil, & : Queue -> _)
      @queue_bindings[{routing_key, nil}].each { |q| yield q }
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      @exchange_bindings[{routing_key, nil}].each { |x| yield x }
    end
  end
end
