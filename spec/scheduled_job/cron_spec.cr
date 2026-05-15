require "../spec_helper"
require "../../src/lavinmq/scheduled_job/cron"

alias Cron = LavinMQ::ScheduledJob::Cron

describe LavinMQ::ScheduledJob::Cron do
  describe ".parse" do
    it "accepts a valid 5-field expression" do
      Cron.parse("* * * * *").should_not be_nil
    end

    it "rejects an expression without 5 fields" do
      expect_raises(Cron::ParseError) { Cron.parse("* * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("") }
    end

    it "rejects out-of-range values" do
      expect_raises(Cron::ParseError) { Cron.parse("60 * * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("* 24 * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("* * 0 * *") }
      expect_raises(Cron::ParseError) { Cron.parse("* * * 13 *") }
      expect_raises(Cron::ParseError) { Cron.parse("* * * * 8") }
    end

    it "rejects malformed parts" do
      expect_raises(Cron::ParseError) { Cron.parse("a * * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("*/0 * * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("5-2 * * * *") }
      expect_raises(Cron::ParseError) { Cron.parse("*,, * * * *") }
    end
  end

  describe "#next_after" do
    it "every minute advances by one minute" do
      cron = Cron.parse("* * * * *")
      cron.next_after(Time.utc(2026, 5, 15, 12, 0, 30)).should eq Time.utc(2026, 5, 15, 12, 1)
      cron.next_after(Time.utc(2026, 5, 15, 12, 0, 0)).should eq Time.utc(2026, 5, 15, 12, 1)
    end

    it "fires on the next matching minute within the same hour" do
      cron = Cron.parse("*/15 * * * *")
      cron.next_after(Time.utc(2026, 5, 15, 12, 0)).should eq Time.utc(2026, 5, 15, 12, 15)
      cron.next_after(Time.utc(2026, 5, 15, 12, 14)).should eq Time.utc(2026, 5, 15, 12, 15)
      cron.next_after(Time.utc(2026, 5, 15, 12, 45)).should eq Time.utc(2026, 5, 15, 13, 0)
    end

    it "weekday-only with hour restriction" do
      # Mon-Fri at 09:00. 2026-05-15 is a Friday, 2026-05-16 a Saturday.
      cron = Cron.parse("0 9 * * 1-5")
      cron.next_after(Time.utc(2026, 5, 15, 8, 0)).should eq Time.utc(2026, 5, 15, 9, 0)
      cron.next_after(Time.utc(2026, 5, 15, 9, 0)).should eq Time.utc(2026, 5, 18, 9, 0)
      cron.next_after(Time.utc(2026, 5, 16, 12, 0)).should eq Time.utc(2026, 5, 18, 9, 0)
    end

    it "Sunday=0 and Sunday=7 are equivalent" do
      a = Cron.parse("0 12 * * 0")
      b = Cron.parse("0 12 * * 7")
      ref = Time.utc(2026, 5, 15, 0, 0)
      a.next_after(ref).should eq b.next_after(ref)
    end

    it "yearly schedule: midnight on Jan 1" do
      cron = Cron.parse("0 0 1 1 *")
      cron.next_after(Time.utc(2026, 5, 15, 12, 0)).should eq Time.utc(2027, 1, 1, 0, 0)
      cron.next_after(Time.utc(2026, 12, 31, 23, 59)).should eq Time.utc(2027, 1, 1, 0, 0)
    end

    it "Feb 29 only fires on leap years" do
      cron = Cron.parse("0 0 29 2 *")
      # 2027 is not a leap year; next is 2028
      cron.next_after(Time.utc(2026, 3, 1, 0, 0)).should eq Time.utc(2028, 2, 29, 0, 0)
    end

    it "DoM and DoW are OR'd when both restricted" do
      # Fires on the 1st of any month OR on any Monday
      cron = Cron.parse("0 0 1 * 1")
      # 2026-05-15 is Friday; next Monday is 2026-05-18
      cron.next_after(Time.utc(2026, 5, 15, 12, 0)).should eq Time.utc(2026, 5, 18, 0, 0)
      # From 2026-05-18 midnight (Mon), next match is 2026-05-25 midnight (next Mon)
      cron.next_after(Time.utc(2026, 5, 18, 0, 0)).should eq Time.utc(2026, 5, 25, 0, 0)
      # From May 30 evening, next is June 1 (day 1) before next Monday June 8
      cron.next_after(Time.utc(2026, 5, 30, 20, 0)).should eq Time.utc(2026, 6, 1, 0, 0)
    end

    it "step over an explicit range" do
      cron = Cron.parse("0 8-18/2 * * *")
      cron.next_after(Time.utc(2026, 5, 15, 7, 0)).should eq Time.utc(2026, 5, 15, 8, 0)
      cron.next_after(Time.utc(2026, 5, 15, 8, 0)).should eq Time.utc(2026, 5, 15, 10, 0)
      cron.next_after(Time.utc(2026, 5, 15, 18, 0)).should eq Time.utc(2026, 5, 16, 8, 0)
    end

    it "comma list of values" do
      cron = Cron.parse("0,30 * * * *")
      cron.next_after(Time.utc(2026, 5, 15, 12, 0)).should eq Time.utc(2026, 5, 15, 12, 30)
      cron.next_after(Time.utc(2026, 5, 15, 12, 30)).should eq Time.utc(2026, 5, 15, 13, 0)
    end
  end

  describe "#to_s" do
    it "returns the original expression" do
      Cron.parse("*/5 * * * *").to_s.should eq "*/5 * * * *"
    end
  end

  describe "additional edge cases" do
    it "accepts boundary values" do
      Cron.parse("0 0 1 1 0").should_not be_nil
      Cron.parse("59 23 31 12 6").should_not be_nil
      Cron.parse("59 23 31 12 7").should_not be_nil # Sunday=7
    end

    it "accepts leading/trailing whitespace" do
      Cron.parse("   */5 * * * *   ").to_s.should eq "*/5 * * * *"
    end

    it "rejects negative numbers" do
      expect_raises(Cron::ParseError) { Cron.parse("-1 * * * *") }
    end

    it "rejects too many fields" do
      expect_raises(Cron::ParseError) { Cron.parse("* * * * * *") }
    end

    it "rejects empty parts in a comma list" do
      expect_raises(Cron::ParseError) { Cron.parse("1,,2 * * * *") }
    end

    it "rejects standalone step without range" do
      expect_raises(Cron::ParseError) { Cron.parse("5/10 * * * *") }
    end

    it "next_after never returns a time <= input" do
      cron = Cron.parse("*/7 * * * *")
      [Time.utc(2026, 5, 15, 12, 0),
       Time.utc(2026, 5, 15, 12, 7),
       Time.utc(2026, 5, 15, 12, 13),
       Time.utc(2026, 5, 15, 23, 59)].each do |t|
        (cron.next_after(t) > t).should be_true
      end
    end

    it "single-value DoW restricts to that day" do
      cron = Cron.parse("0 12 * * 3") # Wednesday
      # 2026-05-15 is Friday; next Wednesday is 2026-05-20
      cron.next_after(Time.utc(2026, 5, 15, 0, 0)).should eq Time.utc(2026, 5, 20, 12, 0)
    end

    it "hour list works across midnight" do
      cron = Cron.parse("0 0,12 * * *")
      cron.next_after(Time.utc(2026, 5, 15, 6, 0)).should eq Time.utc(2026, 5, 15, 12, 0)
      cron.next_after(Time.utc(2026, 5, 15, 18, 0)).should eq Time.utc(2026, 5, 16, 0, 0)
    end

    it "DoM range with step skips correctly" do
      cron = Cron.parse("0 0 1-7/2 * *") # day 1, 3, 5, 7 of each month
      cron.next_after(Time.utc(2026, 5, 2, 0, 0)).should eq Time.utc(2026, 5, 3, 0, 0)
      cron.next_after(Time.utc(2026, 5, 7, 0, 0)).should eq Time.utc(2026, 6, 1, 0, 0)
    end
  end
end
