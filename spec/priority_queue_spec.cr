require "./spec_helper"

describe LavinMQ::AMQP::PriorityQueue do
  it "should prioritize messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio2", props: AMQP::Client::Properties.new(priority: 2)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio1")
      end
    end
  end

  it "should prioritize messages as 0 if no prio is set" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio0"
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        q.publish "prio00"
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio1")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio0")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio00")
      end
    end
  end

  it "should only accept int32 as priority" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => "a"})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should only accept priority >= 0" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 0})
        ch.queue("", args: q_args)
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => -1})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should only accept priority <= 255" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 255})
        ch.queue("", args: q_args)
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => 256})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should not set redelivered to true for a non-requeued message" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        msg = q.get(no_ack: false)
        msg = msg.should_not be_nil
        msg.redelivered.should eq false
      end
    end
  end

  it "should set redelivered to true for a requeued message" do
    with_amqp_server do |s|
      q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
      with_channel(s) do |ch|
        q = ch.queue("q", args: q_args)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        msg = q.get(no_ack: false).should_not be_nil
        msg.reject(requeue: true)
        msg = q.get(no_ack: true).should_not be_nil
        msg.redelivered.should be_true
      end
    end
  end

  context "after restart" do
    it "can restore the priority queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("pq", args: AMQP::Client::Arguments.new({"x-max-priority": 9}))
          q.publish "m1"
          q.publish "m2", props: AMQP::Client::Properties.new(priority: 9)
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m2"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m1"
        end
        s.restart
        with_channel(s) do |ch|
          q = ch.queue("pq", args: AMQP::Client::Arguments.new({"x-max-priority": 9}))
          q.publish "m3", props: AMQP::Client::Properties.new(priority: 8)
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m2"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m3"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m1"
        end
      end
    end
  end
end
