require "./spec_helper"

describe "Delayed Message Exchange" do
  x_name = "delayed-topic"
  delay_q_name = "amq.delayed.#{x_name}"
  x_args = AMQP::Client::Arguments.new({"x-delayed-exchange" => true})

  describe "internal queue" do
    it "should be created with x-dead-letter-exchange" do
      with_channel do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        q = s.vhosts["/"].queues[delay_q_name]?
        q.should_not be_nil
        dlx_exchange = q.not_nil!.arguments["x-dead-letter-exchange"]?.try &.as?(String)
        dlx_exchange.should eq x_name
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  q_name = "delayed_q"
  it "should delay 1 message" do
    with_channel do |ch|
      x = ch.exchange(x_name, "topic", args: x_args)
      q = ch.queue(q_name)
      q.bind(x.name, "#")
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
      x.publish "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      queue = s.vhosts["/"].queues[q_name]
      queue.message_count.should eq 0
      wait_for { queue.message_count == 1 }
      queue.message_count.should eq 1
    end
  ensure
    s.vhosts["/"].delete_exchange(x_name)
    s.vhosts["/"].delete_queue(q_name)
  end

  q_name = "delayed_q"
  it "should delay 2 messages" do
    with_channel do |ch|
      x = ch.exchange(x_name, "topic", args: x_args)
      q = ch.queue(q_name)
      q.bind(x.name, "#")
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
      x.publish "test message 1", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      x.publish "test message 2", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      queue = s.vhosts["/"].queues[q_name]
      queue.message_count.should eq 0
      wait_for { queue.message_count == 2 }
      queue.message_count.should eq 2
      q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
      q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
    end
  ensure
    s.vhosts["/"].delete_exchange(x_name)
    s.vhosts["/"].delete_queue(q_name)
  end

  it "should deliver in correct order" do
    with_channel do |ch|
      x = ch.exchange(x_name, "topic", args: x_args)
      q = ch.queue(q_name)
      q.bind(x.name, "#")
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 1000})
      x.publish "delay-long", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
      x.publish "delay-short", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      queue = s.vhosts["/"].queues[q_name]
      queue.message_count.should eq 0
      wait_for { queue.message_count >= 1 }
      q.get(no_ack: true).try(&.body_io.to_s).should eq("delay-short")
      wait_for { queue.message_count == 1 }
      q.get(no_ack: true).try(&.body_io.to_s).should eq("delay-long")
    end
  ensure
    s.vhosts["/"].delete_exchange(x_name)
    s.vhosts["/"].delete_queue(q_name)
  end

  it "should support x-delayed-message as exchange type" do
    with_channel do |ch|
      xdm_args = AMQP::Client::Arguments.new({"x-delayed-type" => "topic"})
      x = ch.exchange(x_name, "x-delayed-message", args: xdm_args)
      q = ch.queue(q_name)
      q.bind(x.name, "rk")
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 5})
      x.publish "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      queue = s.vhosts["/"].queues[q_name]
      queue.message_count.should eq 0
      wait_for(15.milliseconds) { queue.message_count == 1 }
      queue.message_count.should eq 1
    end
  ensure
    s.vhosts["/"].delete_exchange(x_name)
    s.vhosts["/"].delete_queue(q_name)
  end
end
