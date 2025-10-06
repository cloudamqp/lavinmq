require "amq-protocol"
require "./spec_helper"

describe LavinMQ::QueueFactory do
  with_amqp_server do |s|
    vhost = s.vhosts["/"]

    describe "without x-queue-type" do
      it "should create a non_durable queue" do
        q = LavinMQ::QueueFactory.make(vhost, "q1", durable: false)
        q.is_a?(LavinMQ::Queue).should be_true
      end

      it "should create a durable queue" do
        q = LavinMQ::QueueFactory.make(s.vhosts["/"], "q1", durable: true)
        q.is_a?(LavinMQ::AMQP::DurableQueue).should be_true
      end
    end

    describe "with x-max-priority" do
      it "should create a non_durable priority queue" do
        queue_args = AMQ::Protocol::Table.new({"x-max-priority": 1})
        q = LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args, durable: false)
        q.is_a?(LavinMQ::AMQP::PriorityQueue).should be_true
      end

      it "should create a durable queue" do
        queue_args = AMQ::Protocol::Table.new({"x-max-priority": 1})
        q = LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args, durable: true)
        q.is_a?(LavinMQ::AMQP::PriorityQueue).should be_true
      end
    end

    describe "with x-queue-type=stream" do
      it "should create a stream queue" do
        queue_args = AMQ::Protocol::Table.new({"x-queue-type": "stream"})
        q = LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args)
        q.should be_a LavinMQ::AMQP::Stream
      end

      it "should reject if exclusive" do
        queue_args = AMQ::Protocol::Table.new({"x-queue-type": "stream"})
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args, exclusive: true)
        end
      end

      it "should reject if auto-delete" do
        queue_args = AMQ::Protocol::Table.new({"x-queue-type": "stream"})
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args, auto_delete: true)
        end
      end

      it "should reject x-max-priority argument" do
        queue_args = AMQ::Protocol::Table.new({
          "x-queue-type":   "stream",
          "x-max-priority": 10,
        })
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args, durable: true)
        end
      end
    end

    describe "amqp argument" do
      describe "x-dead-letter-routing-key" do
        it "is rejected if x-dead-letter-exchange is missing" do
          queue_args = LavinMQ::AMQP::Table.new({
            "x-dead-letter-routing-key": "a.b.c",
          })
          expect_raises(LavinMQ::Error::PreconditionFailed) do
            LavinMQ::QueueFactory.make vhost, "q1", arguments: queue_args
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
         Tuple.new, # can't test valid values because x-dead-letter-exchange is required too
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
            it "is accepted when '#{value}'" do
              queue_args = LavinMQ::AMQP::Table.new
              queue_args[header] = value
              q = LavinMQ::QueueFactory.make vhost, "q1", arguments: queue_args
              q.should be_a(LavinMQ::Queue)
            end
          end
          invalid.each do |value|
            it "is rejected when '#{value}'" do
              queue_args = LavinMQ::AMQP::Table.new
              queue_args[header] = value
              expect_raises(LavinMQ::Error::PreconditionFailed) do
                LavinMQ::QueueFactory.make vhost, "q1", arguments: queue_args
              end
            end
          end
        end
      end
    end
  end
end
