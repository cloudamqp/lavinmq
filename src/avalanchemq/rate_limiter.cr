module AvalancheMQ
  abstract class RateItem
    abstract def limited?
    abstract def pristine?
  end

  # Token bucket implementation
  # Fill up tokens to be used for operations, if no tokens are available, we're "limited"
  # 1 sec resolution
class TokenRateItem < RateItem
    @last_used : Time

    def initialize(@max_tokens : Int32, @time : TimeObject.class)
      @tokens = @max_tokens
      @last_used = @time.utc
    end

    def limited?
      now = @time.utc
      @tokens += tokens_to_add(now)
      return true if @tokens == 0
      @last_used = now
      @tokens -= 1
      false
    end

    def pristine?
      (@tokens + tokens_to_add(@time.utc)) == @max_tokens
    end

    private def tokens_to_add(time)
      ((time - @last_used).seconds * @max_tokens).clamp(0, @max_tokens - @tokens)
    end
  end

  abstract class RateLimiter
    abstract def limited?(key : String)

    def allowed?(key : String)
      !limited?(key)
    end
  end

  class SecondsRateLimiter < RateLimiter
    def initialize(@rate_seconds = 1000, time_class : TimeObject.class = RoughTime)
      raise ArgumentError.new("rate must be positive") if @rate_seconds <= 0
      @mutex = Mutex.new
      @pool = Hash(String, RateItem).new { |h,k| h[k] = TokenRateItem.new(@rate_seconds, time_class) }

      # One Fiber per RateLimiter
      spawn(name: "#{self.class} cleanup loop") do
        loop do
          sleep 60
          cleanup
        end
      end
    end

    def limited?(key : String)
      @mutex.synchronize do
        res = @pool[key].limited?
        res
      end
    end

    def cleanup
      @mutex.synchronize do
        @pool.delete_if { |_k,item| item.pristine? }
      end
    end

    def inspect(io)
      io << self.class << "keys=" << @pool.size
    end
  end

  class NoRateLimiter < RateLimiter
    def limited?(key)
      false
    end
  end
end
