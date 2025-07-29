require "./spec_helper"
require "benchmark"

describe "Flow" do
  it "should support consumer flow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
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
        sleep 50.milliseconds # wait little so a new message could be delivered
        msgs.size.should eq 0
        ch.flow(true)
        wait_for { msgs.size == 1 }
        msgs.size.should eq 1
      end
    end
  end

  it "should support server flow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        s.flow(false)
        expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
          q.publish_confirm("m1").should be_false
        end
      end
    end
  end

  it "should stop flow when disk is almost full" do
    LavinMQ::Config.instance.free_disk_min = Int64::MAX
    with_amqp_server do |s|
      s.update_system_metrics(nil)
      s.disk_full?.should be_true
    end
  ensure
    LavinMQ::Config.instance.free_disk_min = 0
  end

  it "should resume flow when disk is no longer full" do
    LavinMQ::Config.instance.free_disk_min = Int64::MAX
    with_amqp_server do |s|
      s.update_system_metrics(nil)
      s.disk_full?.should be_true
      LavinMQ::Config.instance.free_disk_min = 0
      s.update_system_metrics(nil)
      s.disk_full?.should be_false
    end
  ensure
    LavinMQ::Config.instance.free_disk_min = 0
  end
end
