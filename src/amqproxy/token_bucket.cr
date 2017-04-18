module AMQProxy
  class TokenBucket
    def initialize(@length : Int32, @interval : Time::Span)
      @bucket = Channel::Buffered(Nil).new(@length)
      spawn refill_periodically
    end

    def receive
      @bucket.receive
    end

    private def refill_periodically
      loop do
        refill
        sleep @interval
      end
    end

    private def refill
      @length.times do
        break if @bucket.full?
        @bucket.send nil
      end
    end
  end
end
