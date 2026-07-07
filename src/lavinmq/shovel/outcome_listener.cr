require "./constants"

module LavinMQ
  module Shovel
    # A Destination reports each delivery's Outcome to its listener: the Runner
    # (the single place that maps an Outcome to a source action), or a
    # MultiDestinationHandler that intercepts and forwards. Called synchronously
    # for HTTP, and from the publisher-confirm fiber for AMQP on-confirm; never
    # called in NoAck mode (nothing to settle).
    module OutcomeListener
      abstract def report(delivery_tag : UInt64, outcome : Outcome)
    end

    # Default listener for a Destination before the Runner registers, and the
    # sink for NoAck deliveries. Does nothing. A single shared instance so
    # constructing a Destination allocates nothing extra.
    class NullOutcomeListener
      include OutcomeListener

      INSTANCE = new

      def report(delivery_tag : UInt64, outcome : Outcome)
      end
    end
  end
end
