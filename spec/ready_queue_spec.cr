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
end
