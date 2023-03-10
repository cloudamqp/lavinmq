require "./exchange"

module LavinMQ
  class LogExchange < Exchange
    def type : String
      "log"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      @queue_bindings[{routing_key, nil}] << destination

      #channel = ::Log::InMemoryBackend.instance.add_channel
      #handle channel from in_memory_backend
      #while entry = channel.receive
      #  puts "hej"
      #  msg = Message.new("amq.log", ".*", "hej", AMQP::Properties.new)
      #  @vhost.publish(msg: msg)
      #end

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
      @queue_bindings[{routing_key, nil}].each { |q| yield q unless q.internal? }
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      @exchange_bindings[{routing_key, nil}].each { |x| yield x }
    end
  end
end
