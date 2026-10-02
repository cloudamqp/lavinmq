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
    config = LavinMQ::Config.new
    config.blocked_publish_grace = 0
    with_amqp_server(config: config) do |s|
      with_channel(s) do |ch|
        q = ch.queue
        s.flow(false)
        raw_publish(ch, q.name)
        wait_for { ch.closed? }
        ch.@closing_frame.try(&.reply_text).should match /PRECONDITION_FAILED/
        s.vhosts["/"].queue(q.name).message_count.should eq 0
      end
    end
  end

  it "accepts publishes in flight when connection.blocked was sent" do
    with_amqp_server do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      ch = conn.channel
      q = ch.queue
      s.flow(false, "test")
      wait_for { conn.blocked? }
      raw_publish(ch, q.name)
      wait_for { s.vhosts["/"].queue(q.name).message_count == 1 }
      ch.closed?.should be_false
    ensure
      conn.try &.close
    end
  end

  it "accepts publishes while connection.blocked is still waiting to be sent" do
    with_amqp_server do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      ch = conn.channel
      q = ch.queue
      server_client = s.vhosts["/"].connections.first.as(LavinMQ::AMQP::Client)
      # holding the lock keeps the notifier, like one stuck on a slow
      # client, from sending connection.blocked
      server_client.@flow_notify_lock.synchronize do
        s.flow(false, "test")
        raw_publish(ch, q.name)
        wait_for { s.vhosts["/"].queue(q.name).message_count == 1 }
        conn.blocked?.should be_false
      end
      wait_for { conn.blocked? }
      ch.closed?.should be_false
    ensure
      conn.try &.close
    end
  end

  it "rejects publishes after the blocked grace period" do
    config = LavinMQ::Config.new
    config.blocked_publish_grace = 50
    with_amqp_server(config: config) do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      ch = conn.channel
      q = ch.queue
      s.flow(false, "test")
      wait_for { conn.blocked? }
      sleep 100.milliseconds
      raw_publish(ch, q.name)
      wait_for { ch.closed? }
      s.vhosts["/"].queue(q.name).message_count.should eq 0
    ensure
      conn.try &.close
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
