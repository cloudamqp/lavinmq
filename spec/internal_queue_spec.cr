require "./spec_helper"

describe "Internal Queue" do
  q_name = "internal"
  it "should not be possible to consume" do
    expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
      with_channel do |ch|
        q = ch.queue q_name
        Server.vhosts["/"].queues[q_name].internal = true
        q.subscribe(no_ack: false) { true }
      end
    end
  end

  it "should not be possible to publish" do
    with_channel do |ch|
      q = ch.queue q_name
      Server.vhosts["/"].queues[q_name].internal = true
      ch1 = Channel(Tuple(UInt16, String)).new
      ch.on_return do |msg|
        ch1.send({msg.reply_code, msg.reply_text})
      end
      q.publish "msg", mandatory: true
      reply_code, reply_text = ch1.receive
      reply_code.should eq 312
      reply_text.should eq "NO_ROUTE"
    end
  end

  it "should not be possible to bind" do
    with_channel do |ch|
      q = ch.queue q_name
      Server.vhosts["/"].queues[q_name].internal = true
      expect_raises(AMQP::Client::Channel::ClosedException, "ACCESS_REFUSED") do
        q.bind("amq.topic", "foo")
      end
    end
  end

  it "should not be possible to delete" do
    with_channel do |ch|
      q = ch.queue q_name
      Server.vhosts["/"].queues[q_name].internal = true
      expect_raises(AMQP::Client::Channel::ClosedException, "ACCESS_REFUSED") do
        q.delete
      end
    end
  end
end
