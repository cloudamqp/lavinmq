require "./spec_helper"

describe Booth::SequenceTracker do
  it "counts nothing missing for an unbroken sequence" do
    t = Booth::SequenceTracker.new
    1i64.upto(100i64) { |i| t.receive(i) }
    t.received.should eq 100
    t.missing.should eq 0
    t.duplicates.should eq 0
    t.highest.should eq 100
  end

  it "remembers gaps and lets late arrivals fill them" do
    t = Booth::SequenceTracker.new
    t.receive(1)
    t.receive(4)
    t.missing.should eq 2
    t.receive(3)
    t.receive(2)
    t.missing.should eq 0
    t.duplicates.should eq 0
  end

  it "counts redeliveries as duplicates" do
    t = Booth::SequenceTracker.new
    1i64.upto(5i64) { |i| t.receive(i) }
    t.receive(4)
    t.receive(5)
    t.duplicates.should eq 2
    t.missing.should eq 0
  end

  it "only counts a gap as lost after the grace period, if confirmed" do
    t = Booth::SequenceTracker.new
    start = Time.instant
    t.receive(1, start)
    t.receive(5, start) # 2, 3 and 4 missing
    t.check(30.seconds, start + 10.seconds) { true }
    t.lost.should eq 0
    t.check(30.seconds, start + 31.seconds) { |seq| seq != 3 } # 3 still unconfirmed
    t.lost.should eq 2
    t.missing.should eq 1
  end

  it "treats a huge jump as a restart rather than a gap" do
    t = Booth::SequenceTracker.new
    t.receive(1)
    t.receive(5_000_000)
    t.missing.should eq 0
  end
end
