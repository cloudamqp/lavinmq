require "./destination"

module LavinMQ
  module Shovel
    # Coarse failover across a shovel's list of destinations. One destination is
    # active at a time; when it is classified unusable (Abort) or fails to start,
    # the handler advances to the next and the in-flight message is retried there.
    # Only once every destination has failed in a row — with no Confirmed in
    # between — does it emit Abort upward so the Runner errors-out the shovel.
    # Name kept for continuity; this is a failover handler, not fan-out.
    class MultiDestinationHandler < Destination
      @current : Destination?
      @index = 0
      @consecutive_aborts = 0

      def initialize(@destinations : Array(Destination))
      end

      def start
        return if started?
        # Try destinations in order until one starts, so an unreachable primary
        # fails over at startup too.
        @destinations.size.times do |i|
          return if activate(@index + i)
        end
      end

      def stop
        @current.try &.stop
        @current = nil
      end

      def started? : Bool
        if dest = @current
          return dest.started?
        end
        false
      end

      def push(msg)
        if dest = @current
          dest.push(msg)
        else
          # No usable destination at all — surface it rather than dropping.
          @on_outcome.call(msg.delivery_tag, Outcome::Abort)
        end
      end

      # Activate destination at `index`, routing its outcomes through our handler.
      # Returns true if it started.
      private def activate(index) : Bool
        return false if @destinations.empty?
        @index = index % @destinations.size
        dest = @destinations[@index]
        dest.on_outcome = ->(tag : UInt64, outcome : Outcome) { handle(tag, outcome) }
        dest.start
        @current = dest
        true
      rescue ex
        Log.warn { "Destination #{@index} failed to start: #{ex.message}" }
        false
      end

      # Intercepts each active destination's outcome. A non-Abort is forwarded
      # unchanged. An Abort fails over to the next destination (and retries the
      # message there) until all have aborted in a row, then propagates Abort.
      private def handle(tag : UInt64, outcome : Outcome)
        case outcome
        in Outcome::Confirmed, Outcome::Retry, Outcome::Reject
          @consecutive_aborts = 0
          @on_outcome.call(tag, outcome)
        in Outcome::Abort
          @consecutive_aborts += 1
          if @consecutive_aborts >= @destinations.size
            @on_outcome.call(tag, Outcome::Abort) # every destination is unusable
          else
            @current.try &.stop
            start_next
            @on_outcome.call(tag, Outcome::Retry) # re-deliver on the new active one
          end
        end
        nil
      end

      private def start_next
        @destinations.size.times do |i|
          return if activate(@index + 1 + i)
        end
      end

      Log = LavinMQ::Log.for "shovel.multi_destination"
    end
  end
end
