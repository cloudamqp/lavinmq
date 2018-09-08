require "./spec_helper"
require "../src/avalanchemq/federation/upstream"

def setup_qs(conn) : {AMQP::Exchange, AMQP::Queue, AMQP::Queue}
  ch = conn.channel
  x = ch.exchange("", "direct", passive: true)
  q1 = ch.queue("q1")
  q2 = ch.queue("q2")
  {x, q1, q2}
end

def publish(x, rk, msg)
  pmsg = AMQP::Message.new(msg)
  x.publish pmsg, rk
end

describe AvalancheMQ::Upstream do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  it "should federate queue" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    uri = "amqp://localhost"
    vhost = s.not_nil!.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1")

    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    upstream.not_nil!.stop
    close(s)
  end

  it "should not federate queue if no downstream consumer" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    uri = "amqp://localhost"
    vhost = s.not_nil!.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1")

    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      sleep 0.05
      vhost.queues["q1"].message_count.should eq 1
      vhost.queues["q2"].message_count.should eq 0
    end
  ensure
    upstream.not_nil!.stop
    close(s)
  end

  it "should federate queue with ack mode no-ack" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    uri = "amqp://localhost"
    vhost = s.not_nil!.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1",
      ack_mode: AvalancheMQ::Upstream::AckMode::NoAck)

    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    upstream.not_nil!.stop
    close(s)
  end

  it "should federate queue with ack mode on-publish" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    uri = "amqp://localhost"
    vhost = s.not_nil!.vhosts["/"]
    upstream = AvalancheMQ::QueueUpstream.new(vhost, "test", uri, "q1",
      ack_mode: AvalancheMQ::Upstream::AckMode::OnPublish)

    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "federate me"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    upstream.not_nil!.stop
    close(s)
  end
end
