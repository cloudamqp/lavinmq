module LavinMQ
  class StandaloneRunner
    # Yields to the block immediately, then blocks until stop is called.
    def run(&)
      yield
      @stop_channel.receive?
    end

    def stop
      @stop_channel.close
    end

    @stop_channel = Channel(Nil).new
  end
end
