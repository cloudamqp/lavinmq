require "./spec_helper"
require "../src/lavinmq/shovel/shovel_retrier"

describe LavinMQ::Shovel::Retrier do
  describe ".push_with_retry" do
    it "should return true on first successful attempt" do
      attempts = 0
      result = LavinMQ::Shovel::Retrier.push_with_retry(3, 0.001, 0.1) do
        attempts += 1
        true
      end

      result.should be_true
      attempts.should eq 1
    end

    it "should retry until success" do
      attempts = 0
      result = LavinMQ::Shovel::Retrier.push_with_retry(3, 0.001, 0.05) do
        attempts += 1
        attempts == 3
      end

      result.should be_true
      attempts.should eq 3
    end

    it "should return false after max retries" do
      attempts = 0
      result = LavinMQ::Shovel::Retrier.push_with_retry(3, 0.001, 0.05) do
        attempts += 1
        false
      end

      result.should be_false
      attempts.should eq 4 # initial attempt + 3 retries
    end

    it "should apply exponential backoff" do
      attempts = 0
      start_time = Time.monotonic

      LavinMQ::Shovel::Retrier.push_with_retry(2, 0.0, 0.1) do
        attempts += 1
        false
      end

      elapsed = (Time.monotonic - start_time).total_seconds
      # Expected delays: 0.1^1 + 0.1^2 = 0.1 + 0.01 = ~0.11s
      elapsed.should be >= 0.1
      elapsed.should be < 0.2
      attempts.should eq 3
    end

    it "should apply jitter to delays" do
      attempts = 0
      delays = [] of Float64
      last_time = Time.monotonic

      LavinMQ::Shovel::Retrier.push_with_retry(2, 0.01, 0.05) do
        current_time = Time.monotonic
        if attempts > 0
          delays << (current_time - last_time).total_seconds
        end
        last_time = current_time
        attempts += 1
        false
      end

      delays.size.should eq 2
      # First delay: 0.05^1 + jitter(0..0.01) = 0.05-0.06s
      delays[0].should be >= 0.05
      delays[0].should be < 0.07
      # Second delay: 0.05^2 + jitter(0..0.01) = 0.0025-0.0125s
      delays[1].should be >= 0.0
      delays[1].should be < 0.02
    end

    it "should handle zero retries" do
      attempts = 0
      result = LavinMQ::Shovel::Retrier.push_with_retry(0, 0.001, 0.05) do
        attempts += 1
        false
      end

      result.should be_false
      attempts.should eq 1 # only initial attempt
    end

    it "should handle high retry counts" do
      attempts = 0
      result = LavinMQ::Shovel::Retrier.push_with_retry(10, 0.0, 0.02) do
        attempts += 1
        attempts == 5
      end

      result.should be_true
      attempts.should eq 5
    end

    it "should not sleep on first attempt" do
      start_time = Time.monotonic

      LavinMQ::Shovel::Retrier.push_with_retry(3, 0.0, 0.1) do
        true
      end

      elapsed = (Time.monotonic - start_time).total_seconds
      elapsed.should be < 0.01
    end

    it "should respect custom backoff multiplier" do
      attempts = 0
      start_time = Time.monotonic

      LavinMQ::Shovel::Retrier.push_with_retry(2, 0.0, 0.15) do
        attempts += 1
        false
      end

      elapsed = (Time.monotonic - start_time).total_seconds
      # Expected delays: 0.15^1 + 0.15^2 = 0.15 + 0.0225 = ~0.17s
      elapsed.should be >= 0.15
      elapsed.should be < 0.25
    end

    it "should handle exceptions in block" do
      attempts = 0
      expect_raises(Exception, "test error") do
        LavinMQ::Shovel::Retrier.push_with_retry(3, 0.001, 0.05) do
          attempts += 1
          raise Exception.new("test error")
        end
      end

      attempts.should eq 1
    end
  end
end
