require "./spec_helper"
require "../src/avalanchemq/segment_position.cr"

describe AvalancheMQ::SegmentPosition do
  it "should do bitwise concat of segment and position" do
    sp = AvalancheMQ::SegmentPosition.new(0_u32, 0_u32)
    sp.to_i64.should eq 0
    sp = AvalancheMQ::SegmentPosition.new(1_u32, 0_u32)
    sp.to_i64.should eq 2_i64 ** 32
    sp = AvalancheMQ::SegmentPosition.new(1_u32, 4_u32)
    sp.to_i64.should eq 2_i64 ** 32 + 2 ** 2
    sp = AvalancheMQ::SegmentPosition.new(0_u32, 8_u32)
    sp.to_i64.should eq 8
  end

  it "should do bitwise concat of segment and position" do
    sp = AvalancheMQ::SegmentPosition.from_i64(0_i64)
    sp.segment.should eq 0
    sp.position.should eq 0

    sp = AvalancheMQ::SegmentPosition.from_i64(2_i64 ** 32)
    sp.segment.should eq 1
    sp.position.should eq 0

    sp = AvalancheMQ::SegmentPosition.from_i64(8)
    sp.segment.should eq 0
    sp.position.should eq 8
  end
end
