require "./spec_helper"

describe AvalancheMQ::Queue::ReadyQueue do
  it "should insert ordered" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 0u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(1u32, 0u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 1u32))

    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "should insert array ordered" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    sps = Array(AvalancheMQ::SegmentPosition).new
    sps << AvalancheMQ::SegmentPosition.new(1u32, 0u32)
    sps << AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    sps << AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.insert(sps)

    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "keeps track of bytesize" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    rq.bytesize.should eq 0

    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 0u32, bytesize: 1u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(1u32, 0u32, bytesize: 3u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 1u32, bytesize: 2u32))
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
end
