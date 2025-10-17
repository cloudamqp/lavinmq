require "./spec_helper"

describe LavinMQ::Cron do
  describe "#next" do
    it "calculates next run for every minute" do
      cron = LavinMQ::Cron.new("* * * * *")
      now = Time.utc(2025, 1, 1, 12, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 1, 0)
    end

    it "calculates next run for every 5 minutes" do
      cron = LavinMQ::Cron.new("*/5 * * * *")
      now = Time.utc(2025, 1, 1, 12, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 5, 0)
    end

    it "calculates next run for specific minute" do
      cron = LavinMQ::Cron.new("30 * * * *")
      now = Time.utc(2025, 1, 1, 12, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 30, 0)
    end

    it "calculates next run for daily at noon" do
      cron = LavinMQ::Cron.new("0 12 * * *")
      now = Time.utc(2025, 1, 1, 10, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 0, 0)
    end

    it "calculates next run for next day if time passed" do
      cron = LavinMQ::Cron.new("0 12 * * *")
      now = Time.utc(2025, 1, 1, 13, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 2, 12, 0, 0)
    end

    it "calculates next run for first day of month" do
      cron = LavinMQ::Cron.new("0 0 1 * *")
      now = Time.utc(2025, 1, 15, 0, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 2, 1, 0, 0, 0)
    end

    it "handles range expressions" do
      cron = LavinMQ::Cron.new("0-5 * * * *")
      now = Time.utc(2025, 1, 1, 12, 0, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 1, 0)
    end

    it "handles multiple values" do
      cron = LavinMQ::Cron.new("0,15,30,45 * * * *")
      now = Time.utc(2025, 1, 1, 12, 10, 0)
      next_run = cron.next(now)
      next_run.should eq Time.utc(2025, 1, 1, 12, 15, 0)
    end
  end

  describe "#matches?" do
    it "matches exact time" do
      cron = LavinMQ::Cron.new("30 12 * * *")
      time = Time.utc(2025, 1, 1, 12, 30, 0)
      cron.matches?(time).should be_true
    end

    it "doesn't match wrong time" do
      cron = LavinMQ::Cron.new("30 12 * * *")
      time = Time.utc(2025, 1, 1, 12, 31, 0)
      cron.matches?(time).should be_false
    end

    it "matches wildcard" do
      cron = LavinMQ::Cron.new("* * * * *")
      time = Time.utc(2025, 1, 1, 12, 30, 0)
      cron.matches?(time).should be_true
    end
  end

  describe "error handling" do
    it "raises on invalid expression" do
      expect_raises(Exception, /expected 5 fields/) do
        LavinMQ::Cron.new("* * *")
      end
    end

    it "raises on out of range values" do
      expect_raises(Exception, /out of range/) do
        LavinMQ::Cron.new("60 * * * *")
      end
    end
  end
end
