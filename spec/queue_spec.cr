require "./spec_helper"
require "./../src/avalanchemq/queue"

describe AvalancheMQ::Queue do
  it "Should dead letter expiered messages" do
    with_channel do |ch|
      q = ch.queue("ttl", args: AMQP::Client::Arguments.new(
        {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "dlq"}
      ))
      dlq = ch.queue("dlq")
      x = ch.default_exchange
      x.publish_confirm("ttl", q.name)
      msg = wait_for { dlq.get }
      msg.not_nil!.body_io.to_s.should eq "ttl"
      q.get.should eq nil
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
    s.vhosts["/"].delete_queue("ttl")
  end

  it "Should not dead letter messages to it self due to queue length" do
    with_channel do |ch|
      q1 = ch.queue("", args: AMQP::Client::Arguments.new(
        {"x-max-length" => 1, "x-dead-letter-exchange" => ""}
      ))
      q1.publish_confirm ""
      q1.publish_confirm ""
      q1.get.should_not be_nil
      q1.get.should be_nil
    end
  end

  it "Should dead letter messages to it self only if rejected" do
    queue_name = Random::Secure.hex
    with_channel do |ch|
      q1 = ch.queue(queue_name, args: AMQP::Client::Arguments.new(
        {"x-dead-letter-exchange" => ""}
      ))
      ch.default_exchange.publish_confirm("", queue_name)
      msg = q1.get(no_ack: false).not_nil!
      msg.reject(requeue: false)
      q1.get(no_ack: false).should_not be_nil
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
    s.vhosts["/"].delete_queue(queue_name.as(String))
  end

  describe "Paused" do
    x_name = "paused"
    q_name = "paused"
    it "should paused the queue by setting it in flow (get)" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)
        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = s.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        q.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].delete_queue(q_name)
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should paused the queue by setting it in flow (consume)" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)

        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = s.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        channel = Channel(String).new

        # Subscribe on the queue
        # Wait 1 second and unpause the queue, fail test if we get message during that time
        # Make sure the queue continues
        q.subscribe(no_ack: false) do |msg|
          channel.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end
        select
        when msg = channel.receive
          fail "Consumer should not get a message '#{msg}'"
        when timeout 2.seconds
          iq.resume!
        end
        channel.receive.should eq "test message 2"
      end
    ensure
      s.vhosts["/"].delete_queue(q_name)
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should be able to get messages from paused queue with force flag" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)
        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = s.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        x.publish_confirm "test message 3", q.name

        # cannot get from client, queue is paused
        q.get(no_ack: true).should be_nil

        body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
        response = post("/api/queues/%2f/#{q_name}/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.size.should eq 1
        # can get from UI/API even though queue is paused
        body[0]["payload"].should eq "test message 2"

        # resume queue and consume next msg
        iq.resume!
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
      end
    ensure
      s.vhosts["/"].delete_queue(q_name)
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  describe "Message timestamps" do
    it "should be 0 when no messages" do
      queue_name = Random::Secure.hex
      with_channel do |ch|
        ch.queue(queue_name)
        details = s.vhosts["/"].queues[queue_name].details_tuple
        details[:first_message_timestamp].should eq 0
        details[:first_message_timestamp].should eq details[:last_message_timestamp]
      end
    end

    it "should have the same timestamp when one message" do
      queue_name = Random::Secure.hex
      with_channel do |ch|
        q = ch.queue(queue_name)
        q.publish("message 1")
        Fiber.yield
        details = s.vhosts["/"].queues[queue_name].details_tuple
        details[:first_message_timestamp].should eq details[:last_message_timestamp]
      end
    end

    it "should have different timestamp when two messages" do
      queue_name = Random::Secure.hex
      with_channel do |ch|
        q = ch.queue(queue_name)
        q.publish("message 1")
        # Need to sleep for 1 second to get a new RoughTime value.
        sleep 1
        q.publish("message 2")
        sleep 0.1
        details = s.vhosts["/"].queues[queue_name].details_tuple
        details[:ready].should eq 2
        details[:first_message_timestamp].should_not eq details[:last_message_timestamp]
      end
    end
  end
end
