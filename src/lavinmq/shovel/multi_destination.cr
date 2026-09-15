require "./destination"

module LavinMQ
  module Shovel
    # A shovel's list of `dest-uri`s, used one at a time: every start draws one
    # at random and all deliveries of that run go to it. There is no failover.
    # The chosen destination reports its outcomes straight to the Runner, so an
    # Abort counts towards the shovel's abort threshold as it would with a
    # single destination. If the chosen destination cannot start, start raises
    # and the Runner reconnects with backoff; that next start draws again, as
    # does every start after a pause or a reconnect.
    class MultiDestination < Destination
      @current : Destination?

      def initialize(@destinations : Array(Destination))
        raise ArgumentError.new("No destinations configured") if @destinations.empty?
      end

      def start
        return if started?
        dest = @destinations.sample
        dest.listener = @listener
        dest.start
        @current = dest
      end

      def stop
        @current.try &.stop
        @current = nil
      end

      def started? : Bool
        @current.try(&.started?) || false
      end

      def push(msg) : Nil
        dest = @current || raise "Not started"
        dest.push(msg)
      end
    end
  end
end
