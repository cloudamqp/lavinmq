require "./spec_helper"

describe "Persistent Exchange" do
  x_name = "persistent-topic"
  q_name = "amq.persistent.#{x_name}"

  describe "x-offset header" do
    it "should set the x-offset header" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true)
          .try(&.properties.headers)
          .try { |h| h["x-offset"] }
          .should_not be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end
  describe "x-head" do
    it "should retain 1 messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should retain 1 messages, publish 2" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 1})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should retain 2 messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange("persistent-topic")
    end

    it "should skip last messages for head -1" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => -1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange("persistent-topic")
    end

    it "should retain 2 messages, publish 3, request 3, get 2" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 3})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
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
        id = x.publish "test message", q_name
        ch.wait_for_confirm(id)
      end
      close_servers
      TestHelpers.setup
      with_channel do |ch|
        q = ch.queue q_name
        bind_args = AMQP::Client::Arguments.new({"x-head" => 1})
        q.bind(x_name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
        q.get(no_ack: true).should be_nil
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
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should get last 2" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 3})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name
        bind_args = AMQP::Client::Arguments.new({"x-tail" => 2})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should skip first for tail -1" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 3})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name
        bind_args = AMQP::Client::Arguments.new({"x-tail" => -1})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  describe "x-persist-seconds" do
    it "should expire messages" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-ms" => 1000})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
        q.unbind(x.name, "#", args: bind_args)
        sleep 1
        q.bind(x.name, "#", args: bind_args)
        Fiber.yield
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  describe "x-from" do
    it "should get all persisted message from offset" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 3})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name
        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")

        offset = q.get(no_ack: true)
          .try(&.properties.headers)
          .try { |h| h["x-offset"] }

        bind_args = AMQP::Client::Arguments.new({"x-from" => offset})
        q = ch.queue
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should get all persisted message" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name
        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should get all persisted message, same or newer" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 3})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", q.name
        x.publish "test message 2", q.name
        x.publish "test message 3", q.name

        # Publish on other queue to advance segment position
        ch.queue.publish("test message 1")

        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "#", args: bind_args)
        offset = q.get(no_ack: true)
          .try(&.properties.headers)
          .try { |h| h["x-offset"] }

        bind_args = AMQP::Client::Arguments.new({"x-from" => offset.as(Int64) + 1})
        q = ch.queue
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  describe "Exchange to exchange binding" do
    it "should not be allowed" do
      expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
        with_channel do |ch|
          x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
          ch.exchange(x_name, "topic", args: x_args)
          ch.topic_exchange.bind(x_name, "#")
        end
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  describe "Routing" do
    it "should resprect routing key" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-persist-messages" => 2})
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        x.publish "test message 1", "rk.1"
        x.publish "test message 2", "rk.2"
        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "rk.2", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should resprect routing headers" do
      with_channel do |ch|
        hdrs = AMQP::Client::Arguments.new({"x-persist-messages" => 2, "x-match" => "all", "test" => "test"})
        x = ch.exchange(x_name, "headers", args: hdrs)
        q = ch.queue
        x.publish "test message 1", "rk.1", props: AMQP::Client::Properties.new(headers: hdrs)
        hdrs["test"] = "no_match"
        x.publish "test message 2", "rk.2", props: AMQP::Client::Properties.new(headers: hdrs)
        bind_args = AMQP::Client::Arguments.new({"x-from" => 0})
        q.bind(x.name, "#", args: bind_args)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end
end
