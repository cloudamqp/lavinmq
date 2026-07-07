require "./constants"
require "./outcome_listener"

module LavinMQ
  module Shovel
    abstract class Destination
      # The listener a Destination reports delivery Outcomes to (see
      # OutcomeListener). Registered once by the Runner before starting.
      # Defaults to a no-op so an unregistered or NoAck destination reports
      # into the void rather than nil-checking.
      property listener : OutcomeListener = NullOutcomeListener::INSTANCE

      abstract def start

      abstract def stop

      abstract def push(msg)

      abstract def started? : Bool
    end
  end
end
