require "../exchange"

module AvalancheMQ
  class DefaultExchange < Exchange
    def type : String
      "direct"
    end

    def bind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def unbind(destination, routing_key, headers = nil)
      raise "Access refused"
    end

    def do_queue_matches(routing_key, headers = nil, &blk : Queue -> _)
      if q = @vhost.queues[routing_key]?
        yield q unless q.internal?
      end
    end

    def do_exchange_matches(routing_key, headers = nil, &blk : Exchange -> _)
      # noop
    end
  end
end
