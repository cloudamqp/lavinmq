require "./spec_helper"

describe "Persistent Exchange" do
  x_name = "persistent-topic"
  q_name = "amq.persistent.#{x_name}"

  describe "x-head" do
    it "should retain 1 messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message")
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should retain 1 messages, publish 2" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        pmsg = "test message 2"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message 2")
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should retain 2 messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        pmsg = "test message 2"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message 2")
      end
    ensure
      s.vhosts["/"].delete_exchange("persistent-topic")
    end

    it "should skip last messages for head -1" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        pmsg = "test message 2"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => -1})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message")
      end
    ensure
      s.vhosts["/"].delete_exchange("persistent-topic")
    end

    it "should retain 2 messages, publish 3, request 3, get 2" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        pmsg = "test message 2"
        x.publish pmsg, q.name
        pmsg = "test message 3"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 3})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message 2")
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message 3")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    q_name = "replay_test_q"
    it "should retain x messages between restarts" do
      with_channel do |ch|
        ch.confirm_select
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        pmsg = "test message"
        id = x.publish pmsg, q_name
        ch.wait_for_confirm(id)
      end
      close_servers
      TestHelpers.setup
      with_channel do |ch|
        q = ch.queue q_name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x_name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message")
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
      s.vhosts["/"].delete_queue(q_name)
    end
  end

  describe "x-tail" do
    it "should retain 1 messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        pmsg = "test message"
        x.publish pmsg, q.name
        bind_args = AMQP::Client::Arguments.new({"x-tail" => 1})
        q.bind(x.name, "#", args: bind_args)
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.to_s.should eq("test message")
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end
end
