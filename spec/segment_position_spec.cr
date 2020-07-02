require "./spec_helper"
require "../src/avalanchemq/segment_position.cr"

describe AvalancheMQ::SegmentPosition do
  subject = AvalancheMQ::SegmentPosition
  it "should do bitwise concat of segment and position" do
    sp = subject.new(0_u32, 0_u32)
    sp.to_i64.should eq 0
    sp = subject.new(1_u32, 0_u32)
    sp.to_i64.should eq 2_i64 ** 32
    sp = subject.new(1_u32, 4_u32)
    sp.to_i64.should eq 2_i64 ** 32 + 2 ** 2
    sp = subject.new(0_u32, 8_u32)
    sp.to_i64.should eq 8
  end

  it "should do bitwise concat of segment and position" do
    sp = subject.from_i64(0_i64)
    sp.segment.should eq 0
    sp.position.should eq 0

    sp = subject.from_i64(2_i64 ** 32)
    sp.segment.should eq 1
    sp.position.should eq 0

    sp = subject.from_i64(8)
    sp.segment.should eq 0
    sp.position.should eq 8
  end

  describe "with expiration" do
    segment = 0_u32
    position = 0_u32

    it "should create a SP with x-delay" do
      headers = AvalancheMQ::AMQP::Table.new({"x-delay" => 15})
      props = AvalancheMQ::AMQP::Properties.new(headers: headers)
      msg = AvalancheMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 115
      sp.priority.should eq 0
    end

    it "should create a SP with expiration properties" do
      props = AvalancheMQ::AMQP::Properties.new(expiration: "10")
      msg = AvalancheMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 110
      sp.priority.should eq 0
    end

    it "should create a SP with priority and expiration" do
      props = AvalancheMQ::AMQP::Properties.new(expiration: "11", priority: 4_u8)
      msg = AvalancheMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 111
      sp.priority.should eq 4
    end
  end

  it "should create a SP with priority" do
    segment = 0_u32
    position = 0_u32
    props = AvalancheMQ::AMQP::Properties.new(priority: 6_u8)
    msg = AvalancheMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.expiration_ts.should eq 0
    sp.priority.should eq 6
  end

  it "should create a SP without TTL or priority" do
    segment = 0_u32
    position = 0_u32
    props = AvalancheMQ::AMQP::Properties.new
    msg = AvalancheMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.expiration_ts.should eq 0
    sp.priority.should eq 0
  end
end
