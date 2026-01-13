module LavinMQ
  class StandaloneRunner
    # Yields to the block immediately, then blocks until stop is called.
    def run(&)
      yield
      loop do
        select
        when @stop_channel.receive?
          break
        when timeout(30.seconds)
          GC.collect
        end
      end
    end

    def stop
      @stop_channel.close
    end

    @stop_channel = Channel(Nil).new
  end
end
