require "./exchange"

module LavinMQ
  class LogExchange < Exchange
    def type : String
      "log"
    end

    @log_channel : Channel(Log::Entry) = Channel(Log::Entry).new(128)

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination
      @log_channel = ::Log::InMemoryBackend.instance.add_channel
      spawn do
        while entry = @log_channel.receive
          log_text = "#{entry.timestamp} [#{entry.severity.to_s.upcase}] #{entry.source} - #{entry.message}"
          @vhost.publish(msg: Message.new("amq.log", ".*", log_text, AMQP::Properties.new))
        end
      end
      after_bind(destination, routing_key, headers)
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      raise NotImplementedError.new("Not implemented")
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}].delete destination
      ::Log::InMemoryBackend.instance.remove_channel(@log_channel)
      after_unbind(destination, routing_key, headers)
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      raise NotImplementedError.new("Not implemented")
    end

    def do_queue_matches(routing_key, headers = nil, & : Queue -> _)
      @queue_bindings[{routing_key, nil}].each { |q| yield q unless q.internal? }
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      @exchange_bindings[{routing_key, nil}].each { |x| yield x }
    end
  end
end
