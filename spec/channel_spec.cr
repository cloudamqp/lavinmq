require "./spec_helper"

class LavinMQ::AMQP::Consumer
  # Records whether the channel had global prefetch capacity at each recover
  # redelivery, when set
  class_property capacity_on_recover : Array(Bool)? = nil

  def deliver(msg, sp, redelivered = false, recover = false)
    if recover && (recorded = @@capacity_on_recover)
      recorded << @channel.has_capacity?
    end
    previous_def
  end
end

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
end

describe "LavinMQ::AMQP::Channel basic.recover" do
  # The redelivered messages leave @unacked while they're delivered again, so
  # they must still count against the global prefetch, or the channel's
  # consumers could deliver new messages meanwhile
  it "keeps the global prefetch while redelivering with requeue=false" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.prefetch(1, global: true)
        q = ch.queue
        2.times { q.publish "m" }
        msgs = Channel(AMQP::Client::DeliverMessage).new(10)
        q.subscribe(no_ack: false) { |m| msgs.send m }
        msgs.receive
        recorded = LavinMQ::AMQP::Consumer.capacity_on_recover = [] of Bool
        ch.basic_recover(requeue: false)
        msgs.receive.redelivered.should be_true
        recorded.should eq [false]
        server_ch = s.connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        server_ch.has_capacity?.should be_false
      ensure
        LavinMQ::AMQP::Consumer.capacity_on_recover = nil
      end
    end
  end
end
