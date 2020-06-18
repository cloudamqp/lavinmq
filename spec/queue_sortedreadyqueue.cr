require "spec"
require "../src/avalanchemq/segment_position"
require "../src/avalanchemq/queue/ready"

describe AvalancheMQ::Queue::SortedReadyQueue do
  it "should insert SegmentPosition sorted" do
    q = AvalancheMQ::Queue::SortedReadyQueue.new
    sps = [
      AvalancheMQ::SegmentPosition.new(10,10,5),
      AvalancheMQ::SegmentPosition.new(10,10,1),
      AvalancheMQ::SegmentPosition.new(10,10,10),
      AvalancheMQ::SegmentPosition.new(10,10,3)
    ]
    sps.each { |sp| q.push(sp) }
    sps.sort!
    sps.size.times do
      sp1 = sps.shift
      sp2 = q.shift
      sp1.should eq(sp2)
    end
  end

  it "should return SegmentPosition with lowest expiration ts" do
    q = AvalancheMQ::Queue::SortedReadyQueue.new
    sps = [
      AvalancheMQ::SegmentPosition.new(10,10,5),
      AvalancheMQ::SegmentPosition.new(10,10,1),
      AvalancheMQ::SegmentPosition.new(10,10,10),
      AvalancheMQ::SegmentPosition.new(10,10,3)
    ]
    sps.each { |sp| q.push(sp) }
    sp = q.first?
    sp.should eq (AvalancheMQ::SegmentPosition.new(10,10,1))
  end
end
