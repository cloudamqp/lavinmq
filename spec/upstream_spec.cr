require "./spec_helper"
require "../src/avalanchemq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Exchange, AMQP::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("q1")
    q2 = ch.queue("q2")
    {x, q2}
  end

  def self.cleanup
    s.vhosts["/"].delete_queue("q1")
    s.vhosts["/"].delete_queue("q2")
  end

  def self.publish(x, rk, msg)
    pmsg = AMQP::Message.new(msg)
    x.publish pmsg, rk
  end
end

describe AvalancheMQ::Federation::Upstream do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  it "should federate queue" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::QueueUpstream.new(vhost, "test", AMQP_BASE_URL, "q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      UpstreamSpecHelpers.publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end

  it "should not federate queue if no downstream consumer" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::QueueUpstream.new(vhost, "test", AMQP_BASE_URL, "q1")

    with_channel do |ch|
      x = UpstreamSpecHelpers.setup_qs(ch).first
      UpstreamSpecHelpers.publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      sleep 0.05
      vhost.queues["q1"].message_count.should eq 1
      vhost.queues["q2"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end

  it "should federate queue with ack mode no-ack" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::QueueUpstream.new(vhost, "test", AMQP_BASE_URL, "q1",
      ack_mode: AvalancheMQ::Federation::AckMode::NoAck)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      UpstreamSpecHelpers.publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end

  it "should federate queue with ack mode on-publish" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::QueueUpstream.new(vhost, "test", AMQP_BASE_URL, "q1",
      ack_mode: AvalancheMQ::Federation::AckMode::OnPublish)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      UpstreamSpecHelpers.publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end
end
