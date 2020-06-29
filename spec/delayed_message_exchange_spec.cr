require "./spec_helper"

describe "Delayed Message Exchange" do
  x_name = "delayed-topic"
  q_name = "amq.delayed.#{x_name}"
  x_args = AMQP::Client::Arguments.new({"x-delayed-exchange" => true})

  describe "internal queue" do
    it "should be created with x-dead-letter-exchange" do
      with_channel do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[q_name]?.should_not be_nil
        x_dead_letter_exchange = s.vhosts["/"].queues[q_name].arguments["x-dead-letter-exchange"]?.try &.as?(String)
        x_dead_letter_exchange.should eq x_name
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end
  end

  # TODO: specs that messages are delayed and that a message with short delay
  # will be enqueued before message with a longer delay. Also that the current TTL
  # of the internal queue is updated when a new message is published
end

