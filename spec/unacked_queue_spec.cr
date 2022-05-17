require "./spec_helper"

describe LavinMQ::Queue::ReadyQueue do
  it "should calculate message sizes" do
    q = LavinMQ::Queue::UnackQueue.new
    sps = [
      LavinMQ::SegmentPosition.new(10,10,5u32),
      LavinMQ::SegmentPosition.new(10,10,1u32),
      LavinMQ::SegmentPosition.new(10,10,10u32),
      LavinMQ::SegmentPosition.new(10,10,3u32),
      LavinMQ::SegmentPosition.new(10,10,1u32)
    ]
    sps.each { |sp| q.push(sp, nil) }
    q.bytesize.should eq 20u32
    q.avg_bytesize.should eq 4u32
    q.max_bytesize(&.sp.bytesize).should eq 10u32
    q.min_bytesize(&.sp.bytesize).should eq 1u32
    sps.each { |sp| q.purge }
    q.bytesize.should eq 0u32
    q.avg_bytesize.should eq 0u32
    q.max_bytesize(&.sp.bytesize).should eq 0u32
    q.min_bytesize(&.sp.bytesize).should eq 0u32
  end
end
