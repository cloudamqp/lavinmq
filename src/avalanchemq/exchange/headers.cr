require "./exchange"
module AvalancheMQ
  class HeadersExchange < Exchange
    def type : String
      "headers"
    end

    def initialize(@vhost : VHost, @name : String, @durable = false,
                   @auto_delete = false, @internal = false,
                   @arguments = Hash(String, AMQP::Field).new)
      validate!(@arguments)
      super
    end

    def bind(destination : Queue, routing_key, headers)
      validate!(headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @queue_bindings[{routing_key, args}] << destination
      after_bind(destination, routing_key, headers)
    end

    def bind(destination : Exchange, routing_key, headers)
      validate!(headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @exchange_bindings[{routing_key, args}] << destination
      after_bind(destination, routing_key, headers)
    end

    def unbind(destination : Queue, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @queue_bindings[{routing_key, args}].delete destination
      after_unbind(destination, routing_key, headers)
    end

    def unbind(destination : Exchange, routing_key, headers)
      args = headers ? @arguments.merge(headers) : @arguments
      @exchange_bindings[{routing_key, args}].delete destination
      after_unbind(destination, routing_key, headers)
    end

    def do_queue_matches(routing_key, headers = nil, &blk : Queue ->)
      matches(@queue_bindings, routing_key, headers) do |destination|
        q = destination.as(Queue)
        next if q.internal?
        yield q
      end
    end

    def do_exchange_matches(routing_key, headers = nil, &blk : Exchange ->)
      matches(@exchange_bindings, routing_key, headers) { |e| yield e.as(Exchange) }
    end

    private def validate!(headers) : Nil
      if h = headers
        if match = h["x-match"]?
          if match != "all" && match != "any"
            raise Error::PreconditionFailed.new("x-match must be 'any' or 'all'")
          end
        end
      end
    end

    private def matches(bindings, routing_key, headers, &blk : Queue | Exchange ->)
      bindings.each do |bt, dst|
        args = bt[1] || next
        if headers.nil? || headers.empty?
          if args.empty?
            dst.each { |d| yield d }
          end
        else
          case args["x-match"]?
          when "any"
            if args.any? { |k, v| !k.starts_with?("x-") && (headers.has_key?(k) && headers[k] == v) }
              dst.each { |d| yield d }
            end
          else
            if args.all? { |k, v| k.starts_with?("x-") || (headers.has_key?(k) && headers[k] == v) }
              dst.each { |d| yield d }
            end
          end
        end
      end
    end
  end
end
