require "./spec_helper"
require "amqp-client"

describe LavinMQ::AMQP::Channel do
  it "should respect consumer_max_per_channel config" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 1
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      expect_raises(AMQP::Client::Channel::ClosedException, /RESOURCE_ERROR/) do
        channel.basic_consume("test:queue:2") { }
      end
    end
  end

  it "consumer_max_per_channel = 0 allows unlimited consumers" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 0
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      channel.basic_consume("test:queue:1") { }
    end
  end

  it "redelivers to a live consumer on basic.recover without killing the connection" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish "m1"
        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false) { |m| msgs.send m }
        first = msgs.receive
        ch.basic_recover(requeue: false)
        redelivered = msgs.receive
        redelivered.redelivered.should be_true
        redelivered.delivery_tag.should eq first.delivery_tag
      end
    end
  end

  it "keeps the connection open when the channel is closed during basic.recover" do
    with_amqp_server do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      ch = conn.channel
      q = ch.queue("recover-close")
      q.publish_confirm "m1"
      msgs = Channel(AMQP::Client::DeliverMessage).new
      q.subscribe(no_ack: false) { |m| msgs.send m }
      msgs.receive
      sq = s.vhosts["/"].queue("recover-close")
      channel = s.vhosts["/"].connections.first.channels.first.as(LavinMQ::AMQP::Channel)
      sq.@msg_store_lock.synchronize do
        spawn { ch.basic_recover(requeue: false) rescue nil }
        sleep 50.milliseconds # the redelivery now waits for the store lock in Queue#read
        spawn { channel.close("closed during recover") }
        wait_for { !channel.running? }
      end
      # frames are processed in order, so this only succeeds if handling
      # basic.recover didn't close the connection
      conn.channel.queue("recover-close-after")
      conn.closed?.should be_false
      wait_for { sq.message_count == 1 }
    ensure
      conn.try &.close
    end
  end

  it "treats basic.nack of all with nothing outstanding as a no-op" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.basic_nack(0_u64, multiple: true, requeue: true)
        q = ch.queue # the channel must still be usable
        q.publish "m1"
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m1"
      end
    end
  end

  it "lists each unacked delivery once when two consumers share a channel" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("shared")
        2.times { q.publish "m" }
        q.subscribe(no_ack: false) { }
        q.subscribe(no_ack: false) { }
        sq = s.vhosts["/"].queue("shared")
        wait_for { sq.unacked_count == 2 }
        sq.unacked_messages.size.should eq 2
      end
    end
  end
end
