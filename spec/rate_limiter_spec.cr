require "./spec_helper"
require "../src/avalanchemq/rate_limiter"

describe RateLimiter, focus: true do
  rate = 10
  key = "test"

 [TokenRateItem].each do |implementation|
    describe "with #{implementation}" do
      it "should rate limit" do
        limiter = RateLimiter.new(rate, implementation)
        (rate * 2).times.map do
          limiter.limited?(key)
        end.any?.should be_true
      end

      it "should not rate limit" do
        limiter = RateLimiter.new(rate, implementation)
        (rate - 1).times.map do
          limiter.limited?(key)
        end.any?.should be_false
      end

      it "should rate limit, and then not rate limit" do
        limiter = RateLimiter.new(2, implementation)
        limiter.limited?(key).should be_false
        limiter.limited?(key).should be_false
        limiter.limited?(key).should be_true
        sleep 1
        limiter.limited?(key).should be_false
        limiter.limited?(key).should be_false
        limiter.limited?(key).should be_true
      end

      it "should allow all requests" do
        rate = 100
        limiter = RateLimiter.new(rate, implementation)
        res = rate.times.map do
          limiter.limited?(key)
        end
        res.count(false).should eq(rate)
      end

      it "should not limit 50% requests" do
        rate = 50
        limiter = RateLimiter.new(rate, implementation)
        (rate * 2).times.map do
          limiter.limited?(key)
        end.count(false).should eq(rate)
      end

      it "should limit 50% requests" do
        rate = 50
        limiter = RateLimiter.new(rate, implementation)
        (rate * 2).times.map do
          limiter.limited?(key)
        end.count(true).should eq(rate)
      end

      describe "multiple keys" do
        it "should not affect other key" do
          limiter = RateLimiter.new(rate, implementation)

          (rate * 2).times.map do
            limiter.limited?("#{key}-1")
          end.any?.should be_true

          (rate - 1).times.map do
            limiter.limited?("#{key}-2")
          end.any?.should be_false
        end

        it "should purge pristine items on usage" do
          limiter = RateLimiter.new(rate, implementation)
          keys = 5

          keys.times.each do |i|
            limiter.limited?("#{key}-#{i}")
          end

          limiter.inspect.should contain("keys=#{keys}")
          sleep 1
          limiter.limited?("another-key")
          limiter.inspect.should contain("keys=1")
        end
      end
    end
  end
end
