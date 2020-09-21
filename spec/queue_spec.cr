require "./spec_helper"

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
      q1 = ch.queue("q1", args: AMQP::Client::Arguments.new(
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
end
