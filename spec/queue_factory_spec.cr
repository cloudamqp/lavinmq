require "amq-protocol"
require "./spec_helper"

describe LavinMQ::QueueFactory do
  it "should create a non_durable queue" do
    with_amqp_server do |s|
      durable = false
      queue_args = AMQ::Protocol::Table.new({"t" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
        false, false, queue_args)
      q = LavinMQ::QueueFactory.make(s.vhosts["/"], frame)
      q.is_a?(LavinMQ::Queue).should be_true
    end
  end

  it "should create a durable queue" do
    with_amqp_server do |s|
      durable = true
      queue_args = AMQ::Protocol::Table.new({"t" => 1})
      frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
        false, false, queue_args)
      q = LavinMQ::QueueFactory.make(s.vhosts["/"], frame)
      q.is_a?(LavinMQ::AMQP::DurableQueue).should be_true
    end
  end

  describe "priority queues" do
    it "should create a non_durable queue" do
      with_amqp_server do |s|
        durable = false
        queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
        frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
          false, false, queue_args)
        q = LavinMQ::QueueFactory.make(s.vhosts["/"], frame)
        q.is_a?(LavinMQ::AMQP::PriorityQueue).should be_true
      end
    end

    it "should create a durable queue" do
      with_amqp_server do |s|
        durable = true
        queue_args = AMQ::Protocol::Table.new({"x-max-priority" => 1})
        frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, durable, false,
          false, false, queue_args)
        q = LavinMQ::QueueFactory.make(s.vhosts["/"], frame)
        q.is_a?(LavinMQ::AMQP::DurablePriorityQueue).should be_true
      end
    end

    it "should create a stream queue" do
      with_amqp_server do |s|
        queue_args = AMQ::Protocol::Table.new({"x-queue-type" => "stream"})
        frame = AMQ::Protocol::Frame::Method::Queue::Declare.new(0, 0, "test", false, true, false,
          false, false, queue_args)
        q = LavinMQ::QueueFactory.make(s.vhosts["/"], frame)
        q.should be_a LavinMQ::AMQP::Stream
      end
    end
  end

  describe "amqp argument" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]

      describe "x-dead-letter-routing-key" do
        it "is rejected if x-dead-letter-exchange is missing" do
          arguments = LavinMQ::AMQP::Table.new
          arguments["x-dead-letter-routing-key"] = "a.b.c"
          expect_raises(LavinMQ::Error::PreconditionFailed) do
            LavinMQ::QueueFactory.make vhost, "q1", arguments: arguments
          end
        end
      end

      # {Argument name, {valid values}, {invalid values}}
      matrix = {
        {"x-expires",
         {1, 10},
         {0, -1, "str", true}},
        {"x-max-length",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-max-length-bytes",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-message-ttl",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-delivery-limit",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-consumer-timeout",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-cache-size",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-cache-ttl",
         {0, 10},
         {-1, -10, "str", true}},
        {"x-dead-letter-exchange",
         {"str", ""},
         {1, -10, true}},
        {"x-dead-letter-routing-key",
         {"str", ""},
         {1, -10, true}},
        {"x-overflow",
         {"str", ""},
         {1, -10, true}},
        {"x-deduplication-header",
         {"str", ""},
         {1, -10, true}},
      }

      matrix.each do |header, valid, invalid|
        describe header do
          valid.each do |value|
            it "is accepted when #{value}" do
              arguments = LavinMQ::AMQP::Table.new
              arguments[header] = value
              q = LavinMQ::QueueFactory.make vhost, "q1", arguments: arguments
              q.should be_a(LavinMQ::Queue)
            end
          end
          invalid.each do |value|
            it "is rejected when #{value}" do
              arguments = LavinMQ::AMQP::Table.new
              arguments[header] = value
              expect_raises(LavinMQ::Error::PreconditionFailed) do
                LavinMQ::QueueFactory.make vhost, "q1", arguments: arguments
              end
            end
          end
        end
      end
    end
  end
end
