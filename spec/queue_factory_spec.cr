require "amq-protocol"
require "./spec_helper"

describe AvalancheMQ::QueueFactory do
  it "should create a non_durable queue" do
    durable = false
    queue_args = AMQ::Protocol::Table.new({"t" => 1})
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
                                                             false, false, queue_args)
    q = AvalancheMQ::QueueFactory.make(s.vhosts["/"], frame)
    q.is_a?(AvalancheMQ::Queue).should be_true
  end

  it "should create a durable queue" do
    durable = true
    queue_args = AMQ::Protocol::Table.new({"t" => 1})
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
                                                             false, false, queue_args)
    q = AvalancheMQ::QueueFactory.make(s.vhosts["/"], frame)
    q.is_a?(AvalancheMQ::DurableQueue).should be_true
  end

  describe "priority queues" do
    it "should create a non_durable queue" do
      durable = false
      queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
                                                               false, false, queue_args)
      q = AvalancheMQ::QueueFactory.make(s.vhosts["/"], frame)
      q.is_a?(AvalancheMQ::PriorityQueue).should be_true
    end

    it "should create a durable queue" do
      durable = true
      queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
                                                               false, false, queue_args)
      q = AvalancheMQ::QueueFactory.make(s.vhosts["/"], frame)
      q.is_a?(AvalancheMQ::DurablePriorityQueue).should be_true
    end
  end
end
