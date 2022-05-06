require "./spec_helper"
require "../src/lavinmq/segment_position.cr"

describe LavinMQ::SegmentPosition do
  subject = LavinMQ::SegmentPosition
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
      headers = LavinMQ::AMQP::Table.new({"x-delay" => 15})
      props = LavinMQ::AMQP::Properties.new(headers: headers)
      msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 115
      sp.priority.should eq 0
    end

    it "should create a SP with expiration properties" do
      props = LavinMQ::AMQP::Properties.new(expiration: "10")
      msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 110
      sp.priority.should eq 0
    end

    it "should create a SP with priority and expiration" do
      props = LavinMQ::AMQP::Properties.new(expiration: "11", priority: 4_u8)
      msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(segment, position, msg)
      sp.expiration_ts.should eq 111
      sp.priority.should eq 4
    end
  end

  it "should create a SP with priority" do
    segment = 0_u32
    position = 0_u32
    props = LavinMQ::AMQP::Properties.new(priority: 6_u8)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.expiration_ts.should eq 0
    sp.priority.should eq 6
  end

  it "should create a SP without TTL or priority" do
    segment = 0_u32
    position = 0_u32
    props = LavinMQ::AMQP::Properties.new
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.expiration_ts.should eq 0
    sp.priority.should eq 0
  end

  describe LavinMQ::VHost::ReferencedSPs do
    subject = LavinMQ::SegmentPosition

    it "it should always be sorted (ReadyQueue)" do
      offsets = (0..4).to_a.map(&.to_i64)
      sps = offsets.map { |i| LavinMQ::SegmentPosition.from_i64(i) }

      ready = LavinMQ::Queue::ReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = LavinMQ::VHost::ReferencedSPs.new(1)
      ref_sps << LavinMQ::VHost::SPQueue.new(ready)

      ready.to_a.map(&.to_i64).should eq offsets
      ref_sps.to_a.map(&.to_i64).should eq offsets
    end

    it "should always be sorted (ExpirationReadyQueue)" do
      sps = [
        LavinMQ::SegmentPosition.new(0, 0, expiration_ts: 1_i64),
        LavinMQ::SegmentPosition.new(0, 1, expiration_ts: 3_i64),
        LavinMQ::SegmentPosition.new(0, 2, expiration_ts: 1_i64),
        LavinMQ::SegmentPosition.new(0, 3, expiration_ts: 5_i64),
        LavinMQ::SegmentPosition.new(0, 4, expiration_ts: 4_i64),
      ]
      ready = LavinMQ::Queue::ExpirationReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = LavinMQ::VHost::ReferencedSPs.new(1)
      ref_sps << LavinMQ::VHost::SPQueue.new(ready)

      # The ExpirationReadyQueue queue should always be ordered by expiration timestamp
      ready.to_a.map(&.to_i64).should eq [0, 2, 1, 4, 3].map(&.to_i64)

      # The same ready queue but from ReferencesSPs should be ordered by SP (segment + offset)
      ref_sps.to_a.map(&.to_i64).should eq (0..4).to_a.map(&.to_i64)
    end

    it "should always be sorted (PriorityReadyQueue)" do
      sps = [
        LavinMQ::SegmentPosition.new(0, 0, priority: 1_u8),
        LavinMQ::SegmentPosition.new(0, 1, priority: 3_u8),
        LavinMQ::SegmentPosition.new(0, 2, priority: 1_u8),
        LavinMQ::SegmentPosition.new(0, 3, priority: 5_u8),
        LavinMQ::SegmentPosition.new(0, 4, priority: 4_u8),
      ]
      ready = LavinMQ::Queue::PriorityReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = LavinMQ::VHost::ReferencedSPs.new(1)
      ref_sps << LavinMQ::VHost::SPQueue.new(ready)

      # The PriorityReadyQueue queue should always be ordered by priority (highest prio first)
      ready.to_a.map(&.to_i64).should eq [3, 4, 1, 0, 2].map(&.to_i64)

      # The same ready queue but from ReferencesSPs should be ordered by SP (segment + offset)
      ref_sps.to_a.map(&.to_i64).should eq (0..4).to_a.map(&.to_i64)
    end
  end
end
