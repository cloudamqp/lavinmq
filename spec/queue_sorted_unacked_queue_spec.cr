require "spec"
require "../src/lavinmq/queue/unacked"

describe LavinMQ::Queue::UnackQueue do
  it "should insert SegmentPosition sorted" do
    q = LavinMQ::Queue::UnackQueue.new
    sps = [
      LavinMQ::SegmentPosition.new(1, 0),
      LavinMQ::SegmentPosition.new(1, 1),
      LavinMQ::SegmentPosition.new(0, 1),
    ]
    sps.each { |sp| q.push(sp, false, nil) }
    sps.sort!
    q.each_sp do |sp|
      sp.should eq sps.shift
    end
  end
end
