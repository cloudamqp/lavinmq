abstract class RateItem
  abstract def limited?
  abstract def pristine?
end

# Token bucket implementation
# Fill up tokens to be used for operations, if no tokens are available, we're "limited"
# 1 sec resolution
class TokenRateItem < RateItem
  def initialize(@max_tokens : Int32)
    @tokens = @max_tokens
    @last_used = RoughTime.utc
  end

  def limited?
    now = RoughTime.utc
    @tokens += tokens_to_add(now)
    return true if @tokens == 0
    @last_used = now
    @tokens -= 1
    false
  end

  def pristine?
    (@tokens + tokens_to_add(RoughTime.utc)) == @max_tokens
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

# TODO: Thread safety?
# TODO: Allow multiple rates in same limiter (allow to initialize with multiple rates)
# TODO: Use https://github.com/crystal-lang/crystal/pull/8506
class SecondsRateLimiter < RateLimiter
  def initialize(@rate_seconds = 1000)
    raise ArgumentError.new("rate must be positive") if @rate_seconds <= 0
    @mutex = Mutex.new
    @pool = Hash(String, RateItem).new { |h,k| h[k] = TokenRateItem.new(@rate_seconds) }
  end

  def limited?(key : String)
    @mutex.synchronize do
      res = @pool[key].limited?
      @pool.delete_if { |_k,item| item.pristine? }
      res
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
