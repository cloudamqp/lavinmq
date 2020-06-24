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
    @last_used = Time.utc
  end

  def limited?
    now = Time.utc
    @tokens += tokens_to_add(now)
    return true if @tokens == 0
    @last_used = now
    @tokens -= 1
    false
  end

  def pristine?
    (@tokens + tokens_to_add(Time.utc)) == @max_tokens
  end

  private def tokens_to_add(time)
    ((time - @last_used).seconds * @max_tokens).clamp(0, @max_tokens - @tokens)
  end
end

# TODO: Allow multiple rates in same limiter (allow to initialize with multiple rates)
# TODO: Thread safety?
# TODO: Use https://github.com/crystal-lang/crystal/pull/8506
# TODO: increase resolution
class RateLimiter
  def initialize(@rate_seconds : Int32, rate_klass : RateItem.class)
    @pool = Hash(String, RateItem).new { |h,k| h[k] = rate_klass.new(@rate_seconds) }
  end

  def limited?(key : String)
    res = @pool[key].limited?
    @pool.delete_if { |_k,item| item.pristine? }
    res
  end

  def inspect(io)
    io << self.class << "keys=" << @pool.size
  end
end
