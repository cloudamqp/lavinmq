require "./spec_helper"
require "benchmark"

describe "Flow" do
  it "should support consumer flow" do
    with_channel do |ch|
      q = ch.queue
      ch.prefetch 1
      q.publish "msg"
      msgs = Channel(AMQP::Client::Message).new(10)
      q.subscribe(no_ack: false) do |m|
        msgs.send m
      end
      m = msgs.receive
      ch.flow(false)
      m.ack
      q.publish "msg"
      sleep 0.05 # wait little so a new message could be delivered
      received = true
      select
      when msgs.receive
      else
        received = false
      end
      received.should be_false
      ch.flow(true)
      msgs.receive.ack
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
