require "./exchange"

module LavinMQ
  module AMQP
    class DefaultExchange < Exchange
      def type : String
        "direct"
      end

      def bindings_details : Iterator(BindingDetails)
        Iterator(BindingDetails).empty
      end

      protected def bindings(routing_key, headers) : Iterator(LavinMQ::Destination)
        if q = @vhost.queues[routing_key]?
          Tuple(LavinMQ::Destination).new(q).each
        else
          Iterator(LavinMQ::Destination).empty
        end
      end

      def bind(destination, routing_key, headers = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination, routing_key, headers = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end
    end
  end
end
