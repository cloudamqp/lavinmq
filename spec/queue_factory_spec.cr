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

      it "should reject x-max-priority < 0" do
        queue_args = AMQ::Protocol::Table.new({"x-max-priority": -1})
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args)
        end
      end

      it "should reject x-max-priority > 255" do
        queue_args = AMQ::Protocol::Table.new({"x-max-priority": 256})
        expect_raises(LavinMQ::Error::PreconditionFailed) do
          LavinMQ::QueueFactory.make(vhost, "q1", arguments: queue_args)
        end
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

      it "should reject invalid x-max-age" do
        queue_args = AMQ::Protocol::Table.new({
          "x-queue-type": "stream",
          "x-max-age":    "invalid",
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

    describe "to_frame" do
      it "should convert AMQP::Queue to frame" do
        queue_args = LavinMQ::AMQP::Table.new({"x-custom": "value"})
        q = LavinMQ::QueueFactory.make(vhost, "test_queue", durable: true, exclusive: true, auto_delete: false, arguments: queue_args)
        q = q.as(LavinMQ::AMQP::Queue)

        frame = LavinMQ::QueueFactory.to_frame(q)

        frame.queue_name.should eq "test_queue"
        frame.durable.should be_true
        frame.exclusive.should be_true
        frame.auto_delete.should be_false
        frame.arguments.should eq queue_args
      end

      it "should convert MQTT::Session to frame with mqtt queue-type" do
        queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "mqtt"})
        session = LavinMQ::QueueFactory.make(vhost, "mqtt_session", durable: true, auto_delete: false, arguments: queue_args)
        session = session.as(LavinMQ::MQTT::Session)

        frame = LavinMQ::QueueFactory.to_frame(session)

        frame.queue_name.should eq "mqtt_session"
        frame.durable.should be_true
        frame.exclusive.should be_false
        frame.auto_delete.should be_false
        frame.arguments["x-queue-type"]?.should eq "mqtt"
      end

      it "should convert non-durable AMQP::Queue to frame" do
        q = LavinMQ::QueueFactory.make(vhost, "transient_queue", durable: false)
        q = q.as(LavinMQ::AMQP::Queue)

        frame = LavinMQ::QueueFactory.to_frame(q)

        frame.queue_name.should eq "transient_queue"
        frame.durable.should be_false
        frame.exclusive.should be_false
        frame.auto_delete.should be_false
      end

      it "should convert AMQP::Queue with auto_delete to frame" do
        q = LavinMQ::QueueFactory.make(vhost, "auto_delete_queue", durable: false, auto_delete: true)
        q = q.as(LavinMQ::AMQP::Queue)

        frame = LavinMQ::QueueFactory.to_frame(q)

        frame.queue_name.should eq "auto_delete_queue"
        frame.auto_delete.should be_true
      end

      it "should convert MQTT::Session with clean session to frame" do
        queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "mqtt"})
        session = LavinMQ::QueueFactory.make(vhost, "clean_session", durable: false, auto_delete: true, arguments: queue_args)
        session = session.as(LavinMQ::MQTT::Session)

        frame = LavinMQ::QueueFactory.to_frame(session)

        frame.queue_name.should eq "clean_session"
        frame.durable.should be_false
        frame.auto_delete.should be_true
        frame.arguments["x-queue-type"]?.should eq "mqtt"
      end
    end
  end
end
