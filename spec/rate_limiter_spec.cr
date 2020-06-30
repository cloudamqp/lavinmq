require "./spec_helper"
require "../src/avalanchemq/rate_limiter"

describe AvalancheMQ::NoRateLimiter do
  described_class = AvalancheMQ::NoRateLimiter

  it "should never limit" do
    described_class.new.limited?("test")
  end
end

describe AvalancheMQ::SecondsRateLimiter do
  rate = 10
  key = "test"
  described_class = AvalancheMQ::SecondsRateLimiter

  it "should rate limit" do
    limiter = described_class.new(rate)
    (rate * 2).times.map do
      limiter.limited?(key)
    end.any?.should be_true
  end

  it "should not rate limit" do
    limiter = described_class.new(rate)
    (rate - 1).times.map do
      limiter.limited?(key)
    end.any?.should be_false
  end

  it "should rate limit, and then not rate limit" do
    limiter = described_class.new(2)
    limiter.limited?(key).should be_false
    limiter.limited?(key).should be_false
    limiter.limited?(key).should be_true
    sleep 1.4
    limiter.limited?(key).should be_false
    limiter.limited?(key).should be_false
    limiter.limited?(key).should be_true
  end

  it "should allow all requests" do
    rate = 100
    limiter = described_class.new(rate)
    res = rate.times.map do
      limiter.limited?(key)
    end
    res.count(false).should eq(rate)
  end

  it "should not limit 50% requests" do
    rate = 50
    limiter = described_class.new(rate)
    (rate * 2).times.map do
      limiter.limited?(key)
    end.count(false).should eq(rate)
  end

  it "should limit 50% requests" do
    rate = 50
    limiter = described_class.new(rate)
    (rate * 2).times.map do
      limiter.limited?(key)
    end.count(true).should eq(rate)
  end

  describe "Non-positive rate should raise error" do
    [0, -1].each do |bad_rate|
      expect_raises(ArgumentError, "rate must be positive") do
        described_class.new(bad_rate)
      end
    end
  end

  describe "multiple keys" do
    it "should not affect other key" do
      limiter = described_class.new(rate)

      (rate * 2).times.map do
        limiter.limited?("#{key}-1")
      end.any?.should be_true

      (rate - 1).times.map do
        limiter.limited?("#{key}-2")
      end.any?.should be_false
    end

    it "should purge pristine items on usage" do
      limiter = described_class.new(rate)
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
