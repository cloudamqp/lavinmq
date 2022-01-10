require "./spec_helper"
require "benchmark"

describe "Flow" do
  it "should support consumer flow" do
    with_channel do |ch|
      q = ch.queue
      ch.prefetch 1
      q.publish "msg"
      msgs = [] of AMQP::Client::DeliverMessage
      q.subscribe(no_ack: false) do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 1 }
      ch.flow(false)
      msgs.pop.ack
      q.publish "msg"
      sleep 0.05 # wait little so a new message could be delivered
      msgs.size.should eq 0
      ch.flow(true)
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "should support server flow" do
    s.flow(false)
    expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
      with_channel do |ch|
        q = ch.queue
        q.publish_confirm("m1").should be_false
      end
    end
  ensure
    s.flow(true)
  end
end
