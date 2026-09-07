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
      include OutcomeListener

      @current : Destination?
      @index = 0
      @consecutive_aborts = 0

      def initialize(@destinations : Array(Destination))
      end

      # Try destinations in order until one starts, so an unreachable primary
      # fails over at startup too. If none can be started, raise the last
      # failure: to the Runner it is a connection error like any other, retried
      # with backoff.
      def start
        return if started?
        error = nil
        @destinations.size.times do |i|
          error = activate(@index + i)
          return if error.nil?
        end
        raise(error || ArgumentError.new("No destinations configured"))
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
        dest = @current || raise "Not started"
        dest.push(msg)
      end

      # Activate destination at `index`, routing its outcomes through our handler.
      # Returns nil if it started, else the exception its start raised.
      private def activate(index) : Exception?
        @index = index % @destinations.size
        dest = @destinations[@index]
        dest.listener = self
        dest.start
        @current = dest
        nil
      rescue ex
        Log.warn { "Destination #{@index} failed to start: #{ex.message}" }
        ex
      end

      # Intercepts each active destination's outcome. A non-Abort is forwarded
      # unchanged. An Abort fails over to the next destination (and retries the
      # message there) until all have aborted in a row, then propagates Abort.
      def report(delivery_tag : UInt64, outcome : Outcome)
        case outcome
        in Outcome::Confirmed, Outcome::Retry, Outcome::Reject
          @consecutive_aborts = 0
          @listener.report(delivery_tag, outcome)
        in Outcome::Abort
          @consecutive_aborts += 1
          if @consecutive_aborts >= @destinations.size
            @listener.report(delivery_tag, Outcome::Abort) # every destination is unusable
          else
            @current.try &.stop
            start_next
            @listener.report(delivery_tag, Outcome::Retry) # re-deliver on the new active one
          end
        end
      end

      private def start_next
        @destinations.size.times do |i|
          return if activate(@index + 1 + i).nil?
        end
      end

      Log = LavinMQ::Log.for "shovel.multi_destination"
    end
  end
end
