require "./spec_helper"

describe RoughTime do
  it "is close to the real clocks" do
    (RoughTime.utc - Time.utc).abs.should be < 250.milliseconds
    (RoughTime.unix_ms - Time.utc.to_unix_ms).abs.should be < 250
    (RoughTime.instant - Time.instant).abs.should be < 250.milliseconds
  end

  it "keeps ticking while it is read" do
    start = RoughTime.instant
    20.times { sleep 100.milliseconds; RoughTime.unix_ms }
    RoughTime.parked?.should be_false
    (RoughTime.instant - start).should be >= 1.second
  end

  it "parks the ticker when nothing reads the time" do
    RoughTime.instant
    should_eventually(be_true, 3.seconds) { RoughTime.parked? }
  end

  it "returns the current time, not a stale one, when read while parked" do
    RoughTime.instant
    should_eventually(be_true, 3.seconds) { RoughTime.parked? }
    sleep 500.milliseconds # time moves on while parked
    (RoughTime.unix_ms - Time.utc.to_unix_ms).abs.should be < 150
    RoughTime.parked?.should be_false
  end

  it "resumes ticking after being unparked" do
    RoughTime.instant
    should_eventually(be_true, 3.seconds) { RoughTime.parked? }
    start = RoughTime.instant
    5.times { sleep 100.milliseconds; RoughTime.utc }
    (RoughTime.instant - start).should be >= 300.milliseconds
  end
end
