require "spec"
require "../src/avalanchemq/segment_position"

describe AvalancheMQ::SegmentPosition do
  it "should compare first on timestamp" do
    sp1 = AvalancheMQ::SegmentPosition.new(1,1,1)
    sp2 = AvalancheMQ::SegmentPosition.new(1,1,10)

    (sp1 <=> sp2).should be < 0
    (sp2 <=> sp1).should be > 0
    (sp1 < sp2).should be_true
    (sp2 > sp1).should be_true
  end
end
