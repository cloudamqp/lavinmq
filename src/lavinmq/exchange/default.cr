require "./exchange"

module LavinMQ
  class DefaultExchange < Exchange
    def type : String
      "direct"
    end

    def has_binding?(destination : Queue, routing_key : String, headers : Hash(String, AMQP::Field)?)
      false
    end

    def has_binding?(destination : Exchange, routing_key : String, headers : Hash(String, AMQP::Field)?)
      false
    end

    def bind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def unbind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def do_queue_matches(routing_key, headers = nil, & : Queue -> _)
      if q = @vhost.queues[routing_key]?
        yield q
      end
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      # noop
    end
  end
end
