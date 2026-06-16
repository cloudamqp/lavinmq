require "./elector"

module LavinMQ
  module Clustering
    # A cluster of one: the campaign is won by walkover. Yields immediately,
    # then blocks until stop is called.
    class StandaloneElector
      include Elector

      def campaign(& : ->)
        yield
        @stop_channel.receive?
      end

      def stop
        @stop_channel.close
      end

      @stop_channel = Channel(Nil).new
    end
  end
end
