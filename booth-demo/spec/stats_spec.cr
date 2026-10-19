require "./spec_helper"

describe Booth::Meter do
  it "turns counts into per-second rates" do
    m = Booth::Meter.new
    t = Time.instant
    m.tick(t)
    m.mark(50)
    m.tick(t + 1.second)
    m.mark(30)
    m.tick(t + 1.5.seconds)
    m.history[-2].should eq 50.0
    m.rate.should eq 60.0
    m.count.should eq 80
  end

  it "keeps a bounded history" do
    m = Booth::Meter.new
    t = Time.instant
    (Booth::Meter::HISTORY + 10).times { |i| m.tick(t + i.seconds) }
    m.history.size.should eq Booth::Meter::HISTORY
  end
end

describe Booth::Pacer do
  it "hands out roughly the target rate" do
    pacer = Booth::Pacer.new(1000)
    started = Time.instant
    sent = 0
    while Time.instant - started < 200.milliseconds
      sent += pacer.due
    end
    sent.should be_close(200, 40)
  end
end
