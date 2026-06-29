require "./constants"

module LavinMQ
  module Shovel
    abstract class Destination
      # The Runner registers this once, before starting. A Destination invokes
      # it with (delivery_tag, outcome) when a delivery's result is known —
      # synchronously for HTTP, and from the publisher-confirm callback for AMQP
      # on-confirm. In no-ack mode it is never invoked (nothing to settle).
      # The Runner is the single place that maps an Outcome to a source action.
      property on_outcome : Proc(UInt64, Outcome, Nil) = ->(_tag : UInt64, _outcome : Outcome) { nil }

      abstract def start

      abstract def stop

      abstract def push(msg)

      abstract def started? : Bool
    end
  end
end
