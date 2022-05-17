require "./spec_helper"

describe LavinMQ::Queue::ReadyQueue do
  it "should insert ordered" do
    rq = LavinMQ::Queue::ReadyQueue.new
    rq.insert(LavinMQ::SegmentPosition.new(0u32, 0u32))
    rq.insert(LavinMQ::SegmentPosition.new(1u32, 0u32))
    rq.insert(LavinMQ::SegmentPosition.new(0u32, 1u32))

    rq.shift.should eq LavinMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq LavinMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq LavinMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "should insert array ordered" do
    rq = LavinMQ::Queue::ReadyQueue.new
    sps = Array(LavinMQ::SegmentPosition).new
    sps << LavinMQ::SegmentPosition.new(1u32, 0u32)
    sps << LavinMQ::SegmentPosition.new(0u32, 0u32)
    sps << LavinMQ::SegmentPosition.new(0u32, 1u32)
    rq.insert(sps)

    rq.shift.should eq LavinMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq LavinMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq LavinMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "keeps track of bytesize" do
    rq = LavinMQ::Queue::ReadyQueue.new
    rq.bytesize.should eq 0

    rq.insert(LavinMQ::SegmentPosition.new(0u32, 0u32, bytesize: 1u32))
    rq.insert(LavinMQ::SegmentPosition.new(1u32, 0u32, bytesize: 3u32))
    rq.insert(LavinMQ::SegmentPosition.new(0u32, 1u32, bytesize: 2u32))
    rq.bytesize.should eq 6

    sp1 = rq.shift
    rq.bytesize.should eq 5

    sp2 = rq.shift
    rq.bytesize.should eq 3

    # unordered insert
    rq.insert(sp1)
    rq.bytesize.should eq 4

    rq.insert(sp2)
    rq.bytesize.should eq 6

    rq.limit_size(2) { }
    rq.bytesize.should eq 5

    rq.limit_byte_size(2) { }
    rq.bytesize.should eq 0

    rq.insert(sp2)
    rq.insert(sp1)
    rq.bytesize.should eq 3

    rq.delete(sp2)
    rq.bytesize.should eq 1

    rq.shift?
    rq.bytesize.should eq 0

    rq.insert([sp1, sp2])
    rq.bytesize.should eq 3

    rq.purge
    rq.bytesize.should eq 0
  end

  it "should calculate message sizes" do
    rq = LavinMQ::Queue::ReadyQueue.new
    sps = [
      LavinMQ::SegmentPosition.new(10,10,5u32),
      LavinMQ::SegmentPosition.new(10,10,1u32),
      LavinMQ::SegmentPosition.new(10,10,10u32),
      LavinMQ::SegmentPosition.new(10,10,3u32),
      LavinMQ::SegmentPosition.new(10,10,1u32)
    ]
    sps.each { |sp| rq.insert(sp) }
    rq.bytesize.should eq 20u32
    rq.avg_bytesize.should eq 4u32
    rq.max_bytesize(&.bytesize).should eq 10u32
    rq.min_bytesize(&.bytesize).should eq 1u32
  end
end
