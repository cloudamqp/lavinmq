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
      q1 = ch.queue("dlq", args: AMQP::Client::Arguments.new(
        {"x-max-length" => 1, "x-dead-letter-exchange" => "amq.topic", "x-dead-letter-routing-key" => "q2"}
      ))
      q2 = ch.queue("q1")
      x = ch.topic_exchange("amq.topic")
      q1.bind("amq.topic", "#")
      q2.bind("amq.topic", "q2")
      2.times { |i| x.publish_confirm("#{i}", "q1") }
      q1.get.not_nil!.body_io.to_s.should eq "1"
      q1.get.should eq nil
      q2.get.not_nil!.body_io.to_s.should eq "0"
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
    s.vhosts["/"].delete_queue("q1")
  end
end
