require "./spec_helper"
require "../src/avalanchemq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("q1")
    q2 = ch.queue("q2")
    {x, q2}
  end

  def self.cleanup
    s.vhosts["/"].delete_queue("q1")
    s.vhosts["/"].delete_queue("q2")
  end
end

describe AvalancheMQ::Federation::Upstream do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  upstream_vhost = s.vhosts.create("upstream")
  downstream_vhost = s.vhosts.create("downstream")
  upstream_q = "q1"
  downstream_q = "q2"

  it "should federate queue" do
    upstream = AvalancheMQ::Federation::Upstream.new(downstream_vhost,
      "test",
      "amqp:///#{upstream_vhost.name}",
      queue: upstream_q)
    upstream_vhost.declare_queue(upstream_q, durable: false, auto_delete: true)
    msg = AvalancheMQ::Message.new(exchange_name: "", routing_key: upstream_q, body: "federate me")
    upstream_vhost.publish(msg)
    with_channel(vhost: downstream_vhost.name) do |ch|
      q = ch.queue(downstream_q)
      link = upstream.link(downstream_vhost.queues[downstream_q])
      wait_for { link.running? }
      msgs = [] of AMQP::Client::Message
      q.subscribe { |m| msgs << m }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
    downstream_vhost.queues[downstream_q].message_count.should eq 0
    upstream_vhost.queues[upstream_q].message_count.should eq 0
  ensure
    upstream.try &.close
    upstream_vhost.delete_queue(upstream_q)
    downstream_vhost.delete_queue(downstream_q)
  end

  it "should not federate queue if no downstream consumer" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "test", AMQP_BASE_URL, nil, "q1")

    with_channel do |ch|
      x = UpstreamSpecHelpers.setup_qs(ch).first
      x.publish "federate me", "q1"
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
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "test", AMQP_BASE_URL, nil, "q1",
      ack_mode: AvalancheMQ::Federation::AckMode::NoAck)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "q1"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Client::Message
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
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "test", AMQP_BASE_URL, nil, "q1",
      ack_mode: AvalancheMQ::Federation::AckMode::OnPublish)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "q1"
      upstream.link(vhost.queues["q2"])
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end

  it "should resume federation after downstream reconnects" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "test", AMQP_BASE_URL, nil, "q1")
    msgs = [] of AMQP::Client::Message

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "q1"
      upstream.link(vhost.queues["q2"])
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "q1"
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 2 }
      msgs.size.should eq 2
      vhost.queues["q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close
  end

  # it "should federate exchange" do
  #   vhost = s.vhosts["/"]
  #   upstream = AvalancheMQ::Federation::Upstream.new(vhost, "test", AMQP_BASE_URL, "upstream_ex")

  #   with_channel do |ch|
  #     downstream_ex = ch.exchange("downstream_ex", "topic")
  #     downstream_q = ch.queue("downstream_q")
  #     downstream_q.bind(downstream_ex.name, "#")
  #     link = upstream.link(vhost.exchanges[downstream_ex.name])
  #     wait_for { link.running? }
  #     upstream_ex = ch.exchange("upstream_ex", "topic", passive: true)
  #     upstream_ex.publish "federate me", "rk"
  #     msgs = [] of AMQP::Client::Message
  #     downstream_q.subscribe { |msg| msgs << msg }
  #     wait_for { msgs.size == 1 }
  #     msgs.size.should eq 1
  #   end
  # ensure
  #   s.vhosts["/"].delete_queue("downstream_q")
  #   s.vhosts["/"].delete_queue("downstream_ex")
  #   s.vhosts["/"].delete_queue("upstream_ex")
  #   upstream.try &.close
  # end
end
