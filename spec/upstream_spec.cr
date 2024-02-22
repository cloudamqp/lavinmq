require "./spec_helper"
require "../src/lavinmq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("federation_q1")
    q2 = ch.queue("federation_q2")
    {x, q2}
  end

  def self.cleanup(upstream)
    links = upstream.links
    Server.vhosts["/"].delete_queue("federation_q1")
    Server.vhosts["/"].delete_queue("federation_q2")
    wait_for { links.all?(&.state.terminated?) }
  end

  def self.setup_federation(upstream_name, exchange = nil, queue = nil)
    upstream_vhost = Server.vhosts.create("upstream")
    downstream_vhost = Server.vhosts.create("downstream")
    upstream = LavinMQ::Federation::Upstream.new(downstream_vhost, upstream_name,
      "#{AMQP_BASE_URL}/upstream", exchange, queue)
    downstream_vhost.upstreams.not_nil!.add(upstream)
    {upstream, upstream_vhost, downstream_vhost}
  end

  def self.start_link(upstream, pattern = "downstream_ex", applies_to = "exchanges")
    definitions = {"federation-upstream" => JSON::Any.new(upstream.name)} of String => JSON::Any
    upstream.vhost.add_policy("FE", pattern, applies_to, definitions, 12_i8)
  end

  def self.cleanup_ex_federation
    v1 = Server.vhosts["downstream"]
    v2 = Server.vhosts["upstream"]
    Server.vhosts.delete("downstream")
    Server.vhosts.delete("upstream")

    wait_for { !(Dir.exists?(v1.data_dir) || Dir.exists?(v2.data_dir)) }
  end
end

describe LavinMQ::Federation::Upstream do
  it "should use federated queue's name if @queue is empty" do
    vhost_downstream = Server.vhosts.create("/")
    vhost_upstream = Server.vhosts.create("upstream")
    upstream = LavinMQ::Federation::Upstream.new(vhost_downstream, "qf test upstream", "#{AMQP_BASE_URL}/upstream", nil, "")

    with_channel do |ch|
      ch.queue("federated_q")
      link = upstream.link(vhost_downstream.queues["federated_q"])
      wait_for { link.state.running? }
      vhost_upstream.queues.has_key?("federated_q").should be_true
    end
  end

  it "should federate queue" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  end

  it "should not federate queue if no downstream consumer" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream wo downstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x = UpstreamSpecHelpers.setup_qs(ch).first
      x.publish "federate me", "federation_q1"
      link = upstream.link(vhost.queues["federation_q2"])
      wait_for { link.state.running? }
      vhost.queues["federation_q1"].message_count.should eq 1
      vhost.queues["federation_q2"].message_count.should eq 0
    end
  end

  it "should federate queue with ack mode no-ack" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream no-ack", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: LavinMQ::Federation::AckMode::NoAck)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  end

  it "should federate queue with ack mode on-publish" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream on-publish", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: LavinMQ::Federation::AckMode::OnPublish)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  end

  it "should resume federation after downstream reconnects" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream reconnect", AMQP_BASE_URL, nil, "federation_q1")
    msgs = [] of AMQP::Client::DeliverMessage

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
      wait_for { vhost.queues["federation_q1"].message_count == 0 }
    end
  end

  it "should federate exchange" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "ef test upstream", AMQP_BASE_URL, "upstream_ex")

    with_channel do |ch|
      downstream_ex = ch.exchange("downstream_ex", "topic")
      downstream_q = ch.queue("downstream_q")
      downstream_q.bind(downstream_ex.name, "#")
      link = upstream.link(vhost.exchanges[downstream_ex.name])
      wait_for { link.state.running? }
      upstream_ex = ch.exchange("upstream_ex", "topic", passive: true)
      upstream_ex.publish "federate me", "rk"
      msgs = [] of AMQP::Client::DeliverMessage
      downstream_q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "should keep message properties" do
    vhost = Server.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream props", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1", props: AMQP::Client::Properties.new(content_type: "application/json")
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.first.properties.content_type.should eq "application/json"
    end
  end

  it "should federate exchange even with no downstream consumer" do
    upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation("ef test upstream wo downstream", "upstream_ex")
    UpstreamSpecHelpers.start_link(upstream)
    Server.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    Server.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)
    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        downstream_ch.exchange("downstream_ex", "topic")
        wait_for { downstream_vhost.exchanges["downstream_ex"].policy.try(&.name) == "FE" }
        # Assert setup is correct
        wait_for { upstream.links.first?.try &.state.running? }
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "#")
        upstream_ex.publish_confirm "federate me", "rk"
        wait_for { downstream_q.get }
        msgs = [] of AMQP::Client::DeliverMessage
        downstream_q.subscribe { |msg| msgs << msg }
        upstream_ex.publish_confirm "federate me", "rk"
        wait_for { msgs.size == 1 }
        msgs.first.not_nil!.body_io.to_s.should eq("federate me")
      end
    end
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
  end

  it "should continue after upstream restart" do
    upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation("ef test upstream restart", "upstream_ex")
    UpstreamSpecHelpers.start_link(upstream)
    Server.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    Server.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        downstream_ch.exchange("downstream_ex", "topic")
        wait_for { downstream_vhost.exchanges["downstream_ex"].policy.try(&.name) == "FE" }
        # Assert setup is correct
        wait_for { upstream.links.first?.try &.state.running? }
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "#")
        msgs = Channel(String).new
        downstream_q.subscribe do |msg|
          msgs.send msg.body_io.to_s
        end
        upstream_ex.publish_confirm "msg1", "rk1"
        msgs.receive.should eq "msg1"
        sleep 0.01 # allow the downstream federation to ack the msg
        upstream_vhost.connections.each do |conn|
          next unless conn.client_name.starts_with?("Federation link")
          conn.close
        end
        wait_for { upstream.links.first?.try { |l| l.state.stopped? || l.state.starting? } }
        upstream_ex.publish "msg2", "rk2"
        # Should reconnect
        wait_for { upstream.links.first?.try(&.state.running?) }
        upstream_ex.publish "msg3", "rk3"
        msgs.receive.should eq "msg2"
        msgs.receive.should eq "msg3"
      end
    end
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
    UpstreamSpecHelpers.cleanup_ex_federation
  end

  it "should reconnect queue link after upstream restart" do
    upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation("ef test upstream restart", nil, "upstream_q")
    UpstreamSpecHelpers.start_link(upstream, "downstream_q", "queues")
    Server.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    Server.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        downstream_ch.exchange("downstream_ex", "topic")

        upstream_q = upstream_ch.queue("upstream_q")
        downstream_q = downstream_ch.queue("downstream_q")
        wait_for { downstream_vhost.queues["downstream_q"].policy.try(&.name) == "FE" }

        # Assert setup is correct
        wait_for { upstream.links.first?.try &.state.running? }
        msgs = Channel(String).new


        upstream_vhost.connections.each do |conn|
          next unless conn.client_name.starts_with?("Federation link")
          conn.close
        end

        # wait for upstream_connection to be reconnected
        wait_for { upstream.links.first?.try { |l| l.@upstream_connection.try &.closed? == false } }
        wait_for { upstream.links.first?.try(&.state.running?) }

        # publish to upstream & subscribe to downstream
        upstream_q.publish "msg1"
        upstream_q.publish "msg2"
        downstream_q.subscribe do |msg|
          msgs.send msg.body_io.to_s
        end
        msgs.receive.should eq "msg1"
        msgs.receive.should eq "msg2"
      end
    end
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
    UpstreamSpecHelpers.cleanup_ex_federation
  end

  it "should reflect all bindings to upstream q" do
    upstream, upstream_vhost, _ = UpstreamSpecHelpers.setup_federation("ef test bindings", "upstream_ex")
    Server.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    Server.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

    with_channel(vhost: "downstream") do |downstream_ch|
      downstream_ch.exchange("downstream_ex", "topic")
      queues = [] of AMQP::Client::Queue
      10.times do |i|
        downstream_q = downstream_ch.queue("")
        downstream_q.bind("downstream_ex", "before.link.#{i}")
        queues << downstream_q
      end

      UpstreamSpecHelpers.start_link(upstream)
      wait_for { upstream.links.first?.try &.state.running? }

      upstream_q = upstream_vhost.queues.values.first
      upstream_q.bindings.size.should eq queues.size
      # Assert setup is correct
      10.times do |i|
        downstream_q = downstream_ch.queue("")
        downstream_q.bind("downstream_ex", "after.link.#{i}")
        queues << downstream_q
      end
      sleep 0.1
      upstream_q.bindings.size.should eq queues.size
      queues.each &.delete
      sleep 0.01
      upstream_q.bindings.size.should eq 0
    end
  end

  describe "when @queue is nil" do
    describe "#link(Queue)" do
      it "should create link that consumes upstream queue with same name as downstream queue" do
        vhost = Server.vhosts["/"]

        vhost.declare_queue("q1", true, false)
        vhost.declare_queue("q2", true, false)

        upstream = LavinMQ::Federation::Upstream.new(vhost, "test", "amqp://", nil, nil)
        link1 = upstream.link(vhost.queues["q1"])
        link2 = upstream.link(vhost.queues["q2"])

        link1.@upstream_q.should eq "q1"
        link2.@upstream_q.should eq "q2"
      end
    end
  end
end
