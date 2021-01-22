require "./spec_helper"
require "../src/avalanchemq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("federation_q1")
    q2 = ch.queue("federation_q2")
    {x, q2}
  end

  def self.cleanup
    s.vhosts["/"].delete_queue("federation_q1")
    s.vhosts["/"].delete_queue("federation_q2")
  end
end

describe AvalancheMQ::Federation::Upstream do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  it "should federate queue" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close(sync: true)
  end

  it "should not federate queue if no downstream consumer" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream wo downstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x = UpstreamSpecHelpers.setup_qs(ch).first
      x.publish "federate me", "federation_q1"
      link = upstream.link(vhost.queues["federation_q2"])
      wait_for { link.running? }
      vhost.queues["federation_q1"].message_count.should eq 1
      vhost.queues["federation_q2"].message_count.should eq 0
    end
  ensure
    upstream.not_nil!.close(sync: true)
    UpstreamSpecHelpers.cleanup
  end

  it "should federate queue with ack mode no-ack" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream no-ack", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: AvalancheMQ::Federation::AckMode::NoAck)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close(sync: true)
  end

  it "should federate queue with ack mode on-publish" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream on-publish", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: AvalancheMQ::Federation::AckMode::OnPublish)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close(sync: true)
  end

  it "should resume federation after downstream reconnects" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream reconnect", AMQP_BASE_URL, nil, "federation_q1")
    msgs = [] of AMQP::Client::Message

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 2 }
      msgs.size.should eq 2
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close(sync: true)
  end

  it "should federate exchange" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "ef test upstream", AMQP_BASE_URL, "upstream_ex")

    with_channel do |ch|
      downstream_ex = ch.exchange("downstream_ex", "topic")
      downstream_q = ch.queue("downstream_q")
      downstream_q.bind(downstream_ex.name, "#")
      link = upstream.link(vhost.exchanges[downstream_ex.name])
      wait_for { link.running? }
      upstream_ex = ch.exchange("upstream_ex", "topic", passive: true)
      upstream_ex.publish "federate me", "rk"
      msgs = [] of AMQP::Client::Message
      downstream_q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  ensure
    s.vhosts["/"].delete_queue("downstream_q")
    s.vhosts["/"].delete_queue("downstream_ex")
    s.vhosts["/"].delete_queue("upstream_ex")
    upstream.try &.close(sync: true)
  end

  it "should keep message properties" do
    vhost = s.vhosts["/"]
    upstream = AvalancheMQ::Federation::Upstream.new(vhost, "qf test upstream props", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1", props: AMQP::Client::Properties.new(content_type: "application/json")
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.first.properties.content_type.should eq "application/json"
    end
  ensure
    UpstreamSpecHelpers.cleanup
    upstream.not_nil!.close(sync: true)
  end

  it "should federate exchange even with no downstream consumer" do
    upstream_vhost = s.vhosts.create("upstream")
    downstream_vhost = s.vhosts.create("downstream")
    upstream_name = "ef test upstream wo downstream"
    upstream = AvalancheMQ::Federation::Upstream.new(downstream_vhost, upstream_name,
      "#{AMQP_BASE_URL}/upstream", "upstream_ex")
    upstream.ack_timeout = 1.milliseconds
    downstream_vhost.upstreams.not_nil!.add(upstream)
    definitions = {"federation-upstream" => JSON::Any.new(upstream_name)} of String => JSON::Any
    downstream_vhost.add_policy("FE", /downstream_ex/, AvalancheMQ::Policy::Target::Exchanges,
      definitions, 12_i8)

    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        _downstream_ex = downstream_ch.exchange("downstream_ex", "topic")
        wait_for { downstream_vhost.exchanges["downstream_ex"].policy.try(&.name) == "FE" }
        # Assert setup is correct
        wait_for { upstream.links.first?.try(&.running?) }
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "#")
        upstream_ex.publish_confirm "federate me", "rk"
        downstream_q.get.should_not be_nil
        msgs = [] of AMQP::Client::Message
        downstream_q.subscribe { |msg| msgs << msg }
        upstream_ex.publish_confirm "federate me", "rk"
        wait_for { msgs.size == 1 }
        msgs.first.not_nil!.body_io.to_s.should eq("federate me")
      end
    end
    sleep 0.01 # Wait for acks
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
  ensure
    upstream.try &.close(sync: true) # Avoid error log for missing vhost
    s.vhosts.delete("downstream")
    s.vhosts.delete("upstream")
  end
end
