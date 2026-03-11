require "spec"
require "../src/lavinmq/log_rate_limiter"

describe LavinMQ::LogRateLimiter do
  it "yields on first call" do
    limiter = LavinMQ::LogRateLimiter.new(1.second)
    called = false
    limiter.do { called = true }
    called.should be_true
  end

  it "does not yield again within the interval" do
    limiter = LavinMQ::LogRateLimiter.new(1.second)
    count = 0
    3.times { limiter.do { count += 1 } }
    count.should eq 1
  end

  it "yields again after the interval has passed" do
    limiter = LavinMQ::LogRateLimiter.new(50.milliseconds)
    count = 0
    limiter.do { count += 1 }
    sleep 60.milliseconds
    limiter.do { count += 1 }
    count.should eq 2
  end
end
