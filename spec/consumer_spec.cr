require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

describe LavinMQ::Client::Channel::Consumer do
  describe "without x-priority" do
    it "should round-robin" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          msgs = Channel(Int32).new

          q.subscribe(no_ack: false) do |msg|
            msgs.send 1
            msg.ack
          end

          q.subscribe(no_ack: false) do |msg|
            msgs.send 2
            msg.ack
          end

          q.publish("priority")
          q.publish("priority")
          msgs.receive.should eq 1
          msgs.receive.should eq 2
        end
      end
    end
  end

  describe "with x-priority" do
    it "should respect priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
          msgs = Channel(AMQP::Client::DeliverMessage?).new(1)

          q.subscribe(no_ack: false) do |_|
            msgs.send nil
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send msg
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end
        end
      end
    end

    it "should send to prioritized consumer if ready" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
          msgs = Channel(AMQP::Client::DeliverMessage?).new(1)

          q.subscribe(no_ack: false) do |_|
            msgs.send nil
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send msg
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end
        end
      end
    end

    it "should round-robin to high priority consumers" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})

          msgs = Channel(Int32).new
          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send 1
            msg.ack
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send 2
            msg.ack
          end

          100.times { q.publish("priority") }
          50.times do
            msgs.receive.should eq 1
            msgs.receive.should eq 2
          end
        end
      end
    end

    it "should accept any integer as x-priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int8::MIN})) { |_| }
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int16::MAX})) { |_| }
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": 1i64})) { |_| }
        end

        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /out of bounds/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int64::MAX})) { |_| }
          end
        end
      end
    end

    it "should not accept any thing but integers as x-priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /must be an integer/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": "a"})) { |_| }
          end
        end
      end

      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /must be an integer/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": 0.1})) { |_| }
          end
        end
      end
    end
  end
end
