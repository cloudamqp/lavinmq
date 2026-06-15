require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/messages"

alias Control = LavinMQ::Clustering::VR::Control

private def roundtrip(msg)
  io = IO::Memory.new
  msg.to_io(io)
  io.rewind
  Control.read(io)
end

describe LavinMQ::Clustering::VR::Control do
  it "round-trips a Heartbeat" do
    msg = Control::Heartbeat.new(view: 7u64, op: 100u64, commit_op: 95u64, from_id: 3)
    roundtrip(msg).should eq msg
  end

  it "round-trips a StartViewChange" do
    msg = Control::StartViewChange.new(view: 8u64, from_id: 2)
    roundtrip(msg).should eq msg
  end

  it "round-trips a DoViewChange" do
    msg = Control::DoViewChange.new(view: 8u64, last_normal_view: 7u64, op: 100u64, commit_op: 95u64, from_id: 2)
    roundtrip(msg).should eq msg
  end

  it "round-trips a StartView" do
    msg = Control::StartView.new(view: 8u64, primary_id: 2, op: 100u64, commit_op: 95u64, from_id: 1)
    roundtrip(msg).should eq msg
  end

  it "reads consecutive messages framed back-to-back" do
    io = IO::Memory.new
    a = Control::Heartbeat.new(view: 1u64, op: 2u64, commit_op: 1u64, from_id: 9)
    b = Control::StartViewChange.new(view: 2u64, from_id: 9)
    a.to_io(io)
    b.to_io(io)
    io.rewind
    Control.read(io).should eq a
    Control.read(io).should eq b
  end

  it "raises EOFError at end of stream" do
    expect_raises(IO::EOFError) { Control.read(IO::Memory.new) }
  end
end
