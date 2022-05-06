require "spec"
require "../src/avalanchemq/segment_position"
require "../src/avalanchemq/queue/ready"

describe LavinMQ::Queue::ExpirationReadyQueue do
  it "should insert SegmentPosition sorted" do
    q = LavinMQ::Queue::ExpirationReadyQueue.new
    sps = [
      LavinMQ::SegmentPosition.new(10, 10, 5u32),
      LavinMQ::SegmentPosition.new(10, 10, 1u32),
      LavinMQ::SegmentPosition.new(10, 10, 10u32),
      LavinMQ::SegmentPosition.new(10, 10, 3u32),
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
    q = LavinMQ::Queue::ExpirationReadyQueue.new
    sps = [
      LavinMQ::SegmentPosition.new(10, 10, 5u32),
      LavinMQ::SegmentPosition.new(10, 10, 1u32),
      LavinMQ::SegmentPosition.new(10, 10, 10u32),
      LavinMQ::SegmentPosition.new(10, 10, 3u32),
    ]
    sps.each { |sp| q.push(sp) }
    sp = q.first?
    sp.should eq(LavinMQ::SegmentPosition.new(10, 10, 1u32))
  end
end
