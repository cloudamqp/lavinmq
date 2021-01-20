require "./spec_helper"
require "./../src/avalanchemq/queue"

describe AvalancheMQ::Client::Channel::Consumer do
  describe "without x-priority" do
    it "should round-robin" do
      with_channel do |ch|
        q = ch.queue("consumer-priority")
        ch.prefetch 1
        msgs = Channel(Int32).new(2)

        q.subscribe(no_ack: false) do |msg|
          msgs.send 1
          msg.ack
        end

        q.subscribe(no_ack: false) do |msg|
          msgs.send 2
          msg.ack
        end

        q.publish_confirm("priority")
        q.publish_confirm("priority")
        m1 = msgs.receive
        m2 = msgs.receive
        m1.should eq 1
        m2.should eq 2
      end
    ensure
      s.vhosts["/"].delete_queue("consumer-priority")
    end
  end

  describe "with x-priority" do
    it "should respect priority" do
      with_channel do |ch|
        q = ch.queue("consumer-priority")
        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
        msgs = Channel(AMQP::Client::Message?).new(1)

        q.subscribe(no_ack: false) do |_|
          msgs.send nil
        end

        q.subscribe(no_ack: false, args: subscriber_args) do |msg|
          msgs.send msg
        end

        q.publish_confirm("priority")
        m = msgs.receive
        m.should be_truthy
        if m.is_a?(AMQP::Client::Message)
          m.ack
          m.body_io.to_s.should eq "priority"
        end
      end
    ensure
      s.vhosts["/"].delete_queue("consumer-priority")
    end

    it "should send to prioritized consumer if ready" do
      with_channel do |ch|
        q = ch.queue("consumer-priority")
        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
        msgs = Channel(AMQP::Client::Message?).new(1)

        q.subscribe(no_ack: false) do |_|
          msgs.send nil
        end

        q.subscribe(no_ack: false, args: subscriber_args) do |msg|
          msgs.send msg
        end

        q.publish_confirm("priority")
        m = msgs.receive
        m.should be_truthy
        if m.is_a?(AMQP::Client::Message)
          m.ack
          m.body_io.to_s.should eq "priority"
        end

        q.publish_confirm("priority")
        m = msgs.receive
        m.should be_truthy
        if m.is_a?(AMQP::Client::Message)
          m.ack
          m.body_io.to_s.should eq "priority"
        end
      end
    ensure
      s.vhosts["/"].delete_queue("consumer-priority")
    end

    it "should round-robin to high priority consumers" do
      with_channel do |ch|
        q = ch.queue("consumer-priority")
        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
        msgs = Channel(Int32?).new(3)

        q.subscribe(no_ack: false) do |msg|
          msgs.send nil
          msg.ack
        end

        q.subscribe(no_ack: false, args: subscriber_args) do |msg|
          msgs.send 1
          msg.ack
        end

        q.subscribe(no_ack: false, args: subscriber_args) do |msg|
          msgs.send 2
          msg.ack
        end

        q.publish_confirm("priority")
        q.publish_confirm("priority")
        q.publish_confirm("priority")
        m1 = msgs.receive
        m2 = msgs.receive
        m1.should eq 1
        m2.should eq 2
        m1 = msgs.receive
        m1.should eq 1
      end
    ensure
      s.vhosts["/"].delete_queue("consumer-priority")
    end
  end
end
