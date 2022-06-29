require "./spec_helper"

describe LavinMQ::Queue::UnackQueue do
  it "keeps track of bytesize" do
    q = LavinMQ::Queue::UnackQueue.new
    q.bytesize.should eq 0

    sp1 = LavinMQ::SegmentPosition.new(0u32, 0u32, bytesize: 1u32)
    sp2 = LavinMQ::SegmentPosition.new(1u32, 0u32, bytesize: 3u32)
    sp3 = LavinMQ::SegmentPosition.new(0u32, 1u32, bytesize: 2u32)

    q.push(sp1, nil)
    q.push(sp2, nil)
    q.push(sp3, nil)
    q.bytesize.should eq 6

    q.delete(sp1)
    q.bytesize.should eq 5

    q.delete(sp2)
    q.bytesize.should eq 2

    q.push(sp1, nil)
    q.bytesize.should eq 3

    q.push(sp2, nil)
    q.bytesize.should eq 6

    q.purge
    q.bytesize.should eq 0
  end

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
    q.purge
    q.bytesize.should eq 0u32
    q.avg_bytesize.should eq 0u32
    q.max_bytesize(&.sp.bytesize).should eq 0u32
    q.min_bytesize(&.sp.bytesize).should eq 0u32
  end
end
