require "./spec_helper"

describe RoughTime do
  it "utc is close to Time.utc" do
    (Time.utc - RoughTime.utc).abs.should be < 100.milliseconds
  end

  it "unix_ms is close to Time.utc.to_unix_ms" do
    (Time.utc.to_unix_ms - RoughTime.unix_ms).abs.should be < 200
  end

  it "advances without a background updater" do
    utc = RoughTime.utc
    instant = RoughTime.instant
    sleep 50.milliseconds
    (RoughTime.utc - utc).should be >= 30.milliseconds
    (RoughTime.instant - instant).should be >= 30.milliseconds
  end

  it "instant never goes backwards" do
    prev = RoughTime.instant
    1000.times do
      now = RoughTime.instant
      now.should be >= prev
      prev = now
    end
  end
end
