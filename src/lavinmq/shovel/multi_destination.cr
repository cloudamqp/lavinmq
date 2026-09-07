require "./destination"

module LavinMQ
  module Shovel
    # Coarse failover across a shovel's ordered list of destinations. One
    # destination is active at a time; when it is classified unusable (Abort),
    # fails to start, or keeps failing transiently (a run of Retry outcomes),
    # the handler advances to the next and the in-flight message is retried
    # there. Only once every destination has aborted in a row — with no
    # Confirmed in between — does it emit Abort upward so the Runner errors-out
    # the shovel. Name kept for continuity; this is a failover handler, not
    # fan-out.
    class MultiDestinationHandler < Destination
      include OutcomeListener

      # Consecutive Retry outcomes on the active destination before the handler
      # gives another destination a chance. An HTTP destination's start never
      # contacts the endpoint, so a host that is down only ever shows up as
      # connection-refused Retries.
      RETRY_FAILOVER_THRESHOLD = 3

      @current : Destination?
      @index = 0
      @consecutive_aborts = 0
      @consecutive_retries = 0

      def initialize(@destinations : Array(Destination))
      end

      # Try destinations in order until one starts, so an unreachable primary
      # fails over at startup too. If none can be started, raise the last
      # failure: to the Runner it is a connection error like any other, retried
      # with backoff.
      def start
        return if started?
        reset_streaks
        error = nil
        each_index_from(@index) do |i|
          error = activate(i)
          return if error.nil?
        end
        raise(error || ArgumentError.new("No destinations configured"))
      end

      # The list is an ordered preference: the next start begins with the first
      # destination again rather than wherever failover had got to.
      def stop
        @current.try &.stop
        @current = nil
        @index = 0
        reset_streaks
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

      # Yields each destination index once, starting at `base` and wrapping
      # around. `activate` moves @index as it goes, so the walk must not be
      # computed from @index itself or it revisits slots and skips others.
      private def each_index_from(base, &)
        @destinations.size.times do |i|
          yield (base + i) % @destinations.size
        end
      end

      # Activate destination at `index`, routing its outcomes through our handler.
      # Returns nil if it started, else the exception its start raised.
      private def activate(index) : Exception?
        @index = index
        dest = @destinations[index]
        dest.listener = self
        dest.start
        @current = dest
        nil
      rescue ex
        Log.warn { "Destination #{index} failed to start: #{ex.message}" }
        ex
      end

      # Intercepts each active destination's outcome and forwards it, failing
      # over first where the outcome calls for it:
      #   Confirmed / Reject - the destination answered; clear both streaks.
      #   Retry              - forwarded as is; after RETRY_FAILOVER_THRESHOLD in
      #                        a row the redelivery goes to the next destination
      #                        (only when there is another one to go to).
      #   Abort              - fail over for the redelivery. Until every
      #                        destination has aborted in a row that is a Retry;
      #                        from then on Abort propagates so the Runner's abort
      #                        threshold applies, while still rotating.
      def report(delivery_tag : UInt64, outcome : Outcome)
        case outcome
        in Outcome::Confirmed, Outcome::Reject
          reset_streaks
          @listener.report(delivery_tag, outcome)
        in Outcome::Retry
          @consecutive_aborts = 0
          @consecutive_retries += 1
          fail_over if @consecutive_retries >= RETRY_FAILOVER_THRESHOLD && @destinations.size > 1
          @listener.report(delivery_tag, Outcome::Retry)
        in Outcome::Abort
          @consecutive_aborts += 1
          fail_over
          if @consecutive_aborts >= @destinations.size
            @listener.report(delivery_tag, Outcome::Abort) # every destination is unusable
          else
            @listener.report(delivery_tag, Outcome::Retry) # re-deliver on the new active one
          end
        end
      end

      # Stop the active destination and activate the next one that starts.
      private def fail_over
        @consecutive_retries = 0
        @current.try &.stop
        each_index_from(@index + 1) do |i|
          return if activate(i).nil?
        end
      end

      private def reset_streaks
        @consecutive_aborts = 0
        @consecutive_retries = 0
      end

      Log = LavinMQ::Log.for "shovel.multi_destination"
    end
  end
end
