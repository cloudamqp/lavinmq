require "./exchange"

module LavinMQ
  class RandomExchange < Exchange
    def type : String
      "x-random"
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
      queues = @queue_bindings.values.map { |s| s.to_a }.flatten
      without_internal = queues.select { |q| !q.internal? }

      if without_internal.empty?
        return
      end

      yield without_internal.sample
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      exchanges = @exchange_bindings.values.map { |s| s.to_a }.flatten

      if exchanges.empty?
        return
      end

      yield exchanges.sample
    end
  end
end
