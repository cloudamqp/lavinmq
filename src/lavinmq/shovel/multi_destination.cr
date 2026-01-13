require "./destination"

module LavinMQ
  module Shovel
    class MultiDestinationHandler < Destination
      @current_dest : Destination?

      def initialize(@destinations : Array(Destination))
      end

      def start
        return if started?
        next_dest = @destinations.sample
        return unless next_dest
        next_dest.start
        @current_dest = next_dest
      end

      def stop
        @current_dest.try &.stop
        @current_dest = nil
      end

      def push(msg, source)
        @current_dest.try &.push(msg, source)
      end

      def started? : Bool
        if dest = @current_dest
          return dest.started?
        end
        false
      end
    end
  end
end
