module LavinMQ
  module AMQP
    # Scratch state for routing one message: the queues found so far, and the
    # exchanges already visited (stops exchange-to-exchange binding loops).
    #
    # Owned by one publisher (a channel, an MQTT client) and reused for every
    # publish, so the publish path doesn't allocate. Never share one between
    # fibers.
    class RoutingContext
      getter queues : Set(Queue)
      getter exchanges = Set(Exchange).new

      # Pass `queues` to collect into another context's queue set while
      # visiting exchanges afresh, as a CC/BCC pass does.
      def initialize(@queues = Set(Queue).new)
      end

      # Returns false if the exchange was already visited.
      def visit?(exchange : Exchange) : Bool
        @exchanges.add?(exchange)
      end

      def add(queue : Queue) : Nil
        @queues.add(queue)
      end

      # Clears the state after a message is routed.
      # Only Exchange#route_msg calls this.
      def reset : Nil
        @queues.clear
        @exchanges.clear
      end
    end
  end
end
