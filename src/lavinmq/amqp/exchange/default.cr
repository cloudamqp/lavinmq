require "./exchange"

module LavinMQ
  module AMQP
    class DefaultExchange < Exchange
      def type : String
        "direct"
      end

      def bindings_details : Enumerable(BindingDetails)
        Array(BindingDetails).new(0)
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        if q = @vhost.fetch_queue(routing_key)
          yield q
        end
      end

      def bind(destination, routing_key, arguments = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination, routing_key, arguments = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end
    end
  end
end
