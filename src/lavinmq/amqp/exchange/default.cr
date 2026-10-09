require "./exchange"

module LavinMQ
  module AMQP
    class DefaultExchange < Exchange
      NAME = "amq.default"

      def type : String
        "direct"
      end

      def bindings_details : Array(BindingDetails)
        [] of BindingDetails
      end

      def binding_count : Int32
        0
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        if q = @vhost.queue?(routing_key)
          yield q
        end
      end

      def bind(destination, routing_key, arguments = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      def unbind(destination, routing_key, arguments = nil)
        raise LavinMQ::Exchange::AccessRefused.new(self)
      end

      protected def search_value
        NAME
      end

      def details_tuple
        super.merge(name: NAME)
      end
    end
  end
end
