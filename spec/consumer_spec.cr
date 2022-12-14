require "./spec_helper"
require "./../src/lavinmq/queue"

describe LavinMQ::Client::Channel::Consumer do
  describe "without x-priority" do
    it "should round-robin" do
      with_channel do |ch|
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

  describe "with x-priority" do
    it "should respect priority" do
      with_channel do |ch|
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

    it "should send to prioritized consumer if ready" do
      with_channel do |ch|
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

    it "should round-robin to high priority consumers" do
      with_channel do |ch|
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

    it "should accept any integer as x-priority" do
      with_channel do |ch|
        q = ch.queue
        args = AMQP::Client::Arguments.new({"x-priority" => Int8::MIN})
        tag = q.subscribe(args: args) { |_| }
        q.unsubscribe(tag)

        q = ch.queue
        args = AMQP::Client::Arguments.new({"x-priority" => Int16::MAX})
        tag = q.subscribe(args: args) { |_| }
        q.unsubscribe(tag)

        q = ch.queue
        args = AMQP::Client::Arguments.new({"x-priority" => 1i64})
        tag = q.subscribe(args: args) { |_| }
        q.unsubscribe(tag)

        expect_raises(AMQP::Client::Channel::ClosedException, /out of bounds/) do
          q = ch.queue
          args = AMQP::Client::Arguments.new({"x-priority" => Int64::MAX})
          tag = q.subscribe(args: args) { |_| }
          q.unsubscribe(tag)
          ch.prefetch 1
        end
      end
    end
  end
end
