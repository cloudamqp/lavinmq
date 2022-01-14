require "spec"
require "../src/avalanchemq/queue/unacked"

describe AvalancheMQ::Queue::UnackQueue do
  it "should insert SegmentPosition sorted" do
    q = AvalancheMQ::Queue::UnackQueue.new
    sps = [
      AvalancheMQ::SegmentPosition.new(1, 0),
      AvalancheMQ::SegmentPosition.new(1, 1),
      AvalancheMQ::SegmentPosition.new(0, 1),
    ]
    sps.each { |sp| q.push(sp, false, nil) }
    sps.sort!
    q.each_sp do |sp|
      sp.should eq sps.shift
    end
  end
end
