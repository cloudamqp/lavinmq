require "./runner"

module LavinMQ
  class StandaloneRunner < Runner
    # Yields to the block immediately, then blocks until #shutdown.
    def run(& : Clustering::Replicator? -> _)
      yield nil
      @stop_channel.receive?
    end

    private def stop
      @stop_channel.close
    end

    @stop_channel = Channel(Nil).new
  end
end
