module LavinMQ
  struct RateLimiter
    def initialize(@interval : Time::Span)
      @last = Time.instant - @interval
    end

    def do(&)
      now = Time.instant
      if now - @last >= @interval
        @last = now
        yield
      end
    end
  end
end
