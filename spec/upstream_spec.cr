require "./spec_helper"
require "../src/avalanchemq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(conn) : {AMQP::Exchange, AMQP::Queue}
    ch = conn.channel
    x = ch.exchange("", "direct", passive: true)
    q1 = ch.queue("q1")
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

describe AvalancheMQ::Upstream do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  it "should federate queue" do
    uri = "amqp://localhost"
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1")

    AMQP::Connection.start do |conn|
      x, q2 = UpstreamSpecHelpers.setup_qs conn
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
    uri = "amqp://localhost"
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1")

    AMQP::Connection.start do |conn|
      x = UpstreamSpecHelpers.setup_qs(conn).first
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
    uri = "amqp://localhost"
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1",
      ack_mode: AvalancheMQ::Upstream::AckMode::NoAck)

    AMQP::Connection.start do |conn|
      x, q2 = UpstreamSpecHelpers.setup_qs conn
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
    uri = "amqp://localhost"
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1",
      ack_mode: AvalancheMQ::Upstream::AckMode::OnPublish)

    AMQP::Connection.start do |conn|
      x, q2 = UpstreamSpecHelpers.setup_qs conn
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
