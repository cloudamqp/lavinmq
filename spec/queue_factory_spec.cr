require "amq-protocol"
require "./spec_helper"

describe LavinMQ::QueueFactory do
  it "should create a non_durable queue" do
    durable = false
    queue_args = AMQ::Protocol::Table.new({"t" => 1})
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
      false, false, queue_args)
    q = LavinMQ::QueueFactory.make(Server.vhosts["/"], frame)
    q.is_a?(LavinMQ::Queue).should be_true
  end

  it "should create a durable queue" do
    durable = true
    queue_args = AMQ::Protocol::Table.new({"t" => 1})
    frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
      false, false, queue_args)
    q = LavinMQ::QueueFactory.make(Server.vhosts["/"], frame)
    q.is_a?(LavinMQ::DurableQueue).should be_true
  end

  describe "priority queues" do
    it "should create a non_durable queue" do
      durable = false
      queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
        false, false, queue_args)
      q = LavinMQ::QueueFactory.make(Server.vhosts["/"], frame)
      q.is_a?(LavinMQ::PriorityQueue).should be_true
    end

    it "should create a durable queue" do
      durable = true
      queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
        false, false, queue_args)
      q = LavinMQ::QueueFactory.make(Server.vhosts["/"], frame)
      q.is_a?(LavinMQ::DurablePriorityQueue).should be_true
    end

    it "should create a stream queue" do
      queue_args = AMQ::Protocol::Table.new({"x-queue-type" => "stream"})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, true, false,
        false, false, queue_args)
      q = LavinMQ::QueueFactory.make(Server.vhosts["/"], frame)
      q.is_a?(LavinMQ::StreamQueue).should be_true
    end
  end
end
