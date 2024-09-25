require "./exchange"
require "../consistent_hasher.cr"

module LavinMQ
  class ConsistentHashExchange < Exchange
    @hasher = ConsistentHasher(Destination).new

    def type : String
      "x-consistent-hash"
    end

    private def weight(routing_key : String) : UInt32
      routing_key.to_u32? || raise Error::PreconditionFailed.new("Routing key must to be a number")
    end

    private def hash_key(routing_key : String, headers : AMQP::Table?)
      hash_on = @arguments["x-hash-on"]?
      return routing_key unless hash_on.is_a?(String)
      return "" if headers.nil?
      case value = headers[hash_on.as(String)]?
      when String then value.as(String)
      when Nil    then ""
      else             raise Error::PreconditionFailed.new("Routing header must be string")
      end
    end

    def bind(destination : Destination, routing_key : String, headers : AMQP::Table?)
      w = weight(routing_key)
      @hasher.add(destination.name, w, destination)
      ret = case destination
            when Queue
              @queue_bindings[BindingKey.new(routing_key, headers)].add? destination
            when Exchange
              @exchange_bindings[BindingKey.new(routing_key, headers)].add? destination
            end
      after_bind(destination, routing_key, headers)
      ret
    end

    def unbind(destination : Destination, routing_key : String, headers : AMQP::Table?)
      w = weight(routing_key)
      ret = case destination
            when Queue
              @queue_bindings[BindingKey.new(routing_key, headers)].delete destination
            when Exchange
              @exchange_bindings[BindingKey.new(routing_key, headers)].delete destination
            end
      @hasher.remove(destination.name, w)
      ret
    end

    def do_queue_matches(routing_key : String, headers : AMQP::Table?, & : Queue -> _)
      key = hash_key(routing_key, headers)
      case dest = @hasher.get(key)
      when Queue
        yield dest.as(Queue)
      end
    end

    def do_exchange_matches(routing_key : String, headers : AMQP::Table?, & : Exchange -> _)
      key = hash_key(routing_key, headers)
      case dest = @hasher.get(key)
      when Exchange
        yield dest.as(Exchange)
      end
    end
  end
end
