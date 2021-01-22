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

  describe AvalancheMQ::VHost::ReferencedSPs do
    subject = AvalancheMQ::SegmentPosition

    it "it should always be sorted (ReadyQueue)" do
      sps = [
        AvalancheMQ::SegmentPosition.from_i64(0_i64),
        AvalancheMQ::SegmentPosition.from_i64(2_i64),
        AvalancheMQ::SegmentPosition.from_i64(1_i64),
        AvalancheMQ::SegmentPosition.from_i64(3_i64),
      ]
      ready = AvalancheMQ::Queue::ReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = AvalancheMQ::VHost::ReferencedSPs.new(1)
      ref_sps << AvalancheMQ::VHost::SPQueue.new(ready)

      i = 0_i64
      ready.each do |sp|
        sp.to_i64.should eq i
        i += 1
      end

      i = 0_i64
      ref_sps.each do |sp|
        sp.to_i64.should eq i
        i += 1
      end
    end

    it "should always be sorted (ExpirationReadyQueue)" do
      sps = [
        AvalancheMQ::SegmentPosition.new(0, 0, expiration_ts: 1_i64),
        AvalancheMQ::SegmentPosition.new(0, 1, expiration_ts: 3_i64),
        AvalancheMQ::SegmentPosition.new(0, 2, expiration_ts: 1_i64),
        AvalancheMQ::SegmentPosition.new(0, 3, expiration_ts: 5_i64),
        AvalancheMQ::SegmentPosition.new(0, 4, expiration_ts: 4_i64)
      ]
      ready = AvalancheMQ::Queue::ExpirationReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = AvalancheMQ::VHost::ReferencedSPs.new(1)
      ref_sps << AvalancheMQ::VHost::SPQueue.new(ready)

      # expected order when sorted  on SP
      sp_sorted = [0_i64, 1_i64, 2_i64, 3_i64, 4_i64]
      # expected order when sorted onexpiration_ts
      expiration_sorted =[0_i64, 2_i64, 1_i64, 4_i64, 3_i64]

      # The ExpirationReadyQueue queue should always be ordered by expiration timestamp
      i = 0_i64
      ready.each do |sp|
        sp.to_i64.should eq expiration_sorted[i]
        i += 1
      end

      # The same ready queue but from ReferencesSPs should be ordered by SP (segment + offset)
      i = 0_i64
      ref_sps.each do |sp|
        sp.to_i64.should eq sp_sorted[i]
        i += 1
      end
    end

    it "should always be sorted (PriorityReadyQueue)"  do
      sps = [
        AvalancheMQ::SegmentPosition.new(0, 0, priority: 1_u8),
        AvalancheMQ::SegmentPosition.new(0, 1, priority: 3_u8),
        AvalancheMQ::SegmentPosition.new(0, 2, priority: 1_u8),
        AvalancheMQ::SegmentPosition.new(0, 3, priority: 5_u8),
        AvalancheMQ::SegmentPosition.new(0, 4, priority: 4_u8)
      ]
      ready = AvalancheMQ::Queue::PriorityReadyQueue.new(sps.size)
      ready.insert(sps)

      ref_sps = AvalancheMQ::VHost::ReferencedSPs.new(1)
      ref_sps << AvalancheMQ::VHost::SPQueue.new(ready)

      # expected order when sorted  on SP
      sp_sorted = [0_i64, 1_i64, 2_i64, 3_i64, 4_i64]
      # expected order when sorted onexpiration_ts
      priority_sorted =[3_i64, 4_i64, 1_i64, 2_i64, 0_i64]

      # The ExpirationReadyQueue queue should always be ordered by expiration timestamp
      i = 0_i64
      ready.each do |sp|
        sp.to_i64.should eq priority_sorted[i]
        i += 1
      end

      # The same ready queue but from ReferencesSPs should be ordered by SP (segment + offset)
      i = 0_i64
      ref_sps.each do |sp|
        sp.to_i64.should eq sp_sorted[i]
        i += 1
      end
    end
  end
end
