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
  ensure
    s.vhosts["/"].delete_queue("q1")
  end

  it "Should dead letter messages to it self only if rejected" do
    with_channel do |ch|
      q1 = ch.queue("q1", args: AMQP::Client::Arguments.new(
        {"x-dead-letter-exchange" => ""}
      ))
      ch.default_exchange.publish_confirm("", "q1")
      msg = q1.get(no_ack: false).not_nil!
      msg.reject(requeue: false)
      q1.get(no_ack: false).should_not be_nil
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
    s.vhosts["/"].delete_queue("q1")
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
        q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("test message")

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
        q.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("test message")

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
        when channel.receive
          fail "Consumer should not get a message" unless iq.state == AvalancheMQ::QueueState::Flow
        when timeout Time::Span.new(seconds: 2)
          iq.resume!
        end
        channel.receive.should eq "test message 2"
      end
    ensure
      s.vhosts["/"].delete_queue(q_name)
      s.vhosts["/"].delete_exchange(x_name)
    end
  end
end
