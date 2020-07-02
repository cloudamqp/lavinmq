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
  it "should delay messages" do
    with_channel do |ch|
      x = ch.exchange(x_name, "topic", args: x_args)
      q = ch.queue(q_name)
      q.bind(x.name, "#")
      hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
      x.publish "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
      s.vhosts["/"].queues[q_name].message_count.should eq 0
      sleep 2
      s.vhosts["/"].queues[q_name].message_count.should eq 1
    end
  ensure
    s.vhosts["/"].delete_exchange(x_name)
    s.vhosts["/"].delete_queue(q_name)
  end

  # TODO: specs that messages are delayed and that a message with short delay
  # will be enqueued before message with a longer delay. Also that the current TTL
  # of the internal queue is updated when a new message is published
end
