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
    Server.flow(false)
    expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
      with_channel do |ch|
        q = ch.queue
        q.publish_confirm("m1").should be_false
      end
    end
  ensure
    Server.flow(true)
  end

  it "should stop flow when disk is almost full" do
    wait_for { Server.disk_total > 0 }
    LavinMQ::Config.instance.free_disk_min = Int64::MAX
    Server.is_disk_full?.should be_true
  ensure
    LavinMQ::Config.instance.free_disk_min = 0
    Server.flow(true)
  end

  it "should resume flow when disk is no longer full" do
    wait_for { Server.disk_total > 0 }
    LavinMQ::Config.instance.free_disk_min = Int64::MAX
    Server.is_disk_full?.should be_true

    LavinMQ::Config.instance.free_disk_min = 0
    Server.is_disk_full?.should be_false
  ensure
    LavinMQ::Config.instance.free_disk_min = 0
    Server.flow(true)
  end
end
