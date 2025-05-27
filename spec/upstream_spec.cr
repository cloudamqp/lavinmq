require "./spec_helper"
require "../src/lavinmq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("federation_q1")
    q2 = ch.queue("federation_q2")
    {x, q2}
  end

  def self.setup_federation(s, upstream_name, exchange = nil, queue = nil, prefetch = 1000_u16)
    self.cleanup_vhosts(s)
    upstream_vhost = s.vhosts.create("upstream")
    downstream_vhost = s.vhosts.create("downstream")
    upstream = LavinMQ::Federation::Upstream.new(
      downstream_vhost, upstream_name,
      "#{s.amqp_url}/upstream", exchange, queue,
      LavinMQ::Federation::AckMode::OnConfirm, nil, 1_i64, nil, prefetch
    )
    downstream_vhost.upstreams.not_nil!.add(upstream)
    {upstream, upstream_vhost, downstream_vhost}
  end

  def self.start_link(upstream, pattern = "downstream_ex", applies_to = "exchanges")
    definitions = {"federation-upstream" => JSON::Any.new(upstream.name)} of String => JSON::Any
    upstream.vhost.add_policy("FE", pattern, applies_to, definitions, 12_i8)
  end

  def self.cleanup_vhost(s, vhost_name)
    v = s.vhosts[vhost_name]
    s.vhosts.delete(vhost_name)
    wait_for { !(Dir.exists?(v.data_dir)) }
  rescue KeyError
  end

  def self.cleanup_vhosts(s)
    cleanup_vhost(s, "upstream")
    cleanup_vhost(s, "downstream")
  end

  def self.create_fe_chain(s : LavinMQ::Server, chain_length = 10, max_hops = chain_length)
    url = URI.parse(s.amqp_url)
    # Create ten vhosts, each with an exchange "fe"
    # Setup a chain of exchange federation with v10 as downstream
    # and v1 as "top upstream"
    # Messages should travel
    # v1:fe -> v2:fe -> v3:fe -> ... -> v4:fe -> v10:fe
    vhosts = [] of LavinMQ::VHost
    upstreams = [] of LavinMQ::Federation::Upstream
    vhost1 = s.vhosts.create("v1")
    vhost1.declare_exchange("fe", "topic", durable: true, auto_delete: false)
    vhosts << vhost1
    vhost_prev = vhost1
    2.to(chain_length) do |i|
      vhost = s.vhosts.create("v#{i}")
      vhosts << vhost
      vhost.declare_exchange("fe", "topic", durable: true, auto_delete: false)
      url.path = vhost_prev.name
      upstream = LavinMQ::Federation::Upstream.new(vhost, "ef from #{vhost_prev.name}", url.to_s, exchange: nil, queue: nil, max_hops: max_hops)
      upstreams << upstream
      link = upstream.link(vhost.exchanges["fe"])
      wait_for { link.state.running? }
      vhost_prev = vhost
    end
    {vhosts, upstreams}
  end
end

describe LavinMQ::Federation::Upstream do
  describe "Queue federation" do
    it "should use federated queue's name if @queue is empty" do
      with_amqp_server do |s|
        vhost_downstream = s.vhosts.create("/")
        vhost_upstream = s.vhosts.create("upstream")
        upstream = LavinMQ::Federation::Upstream.new(vhost_downstream, "qf test upstream", "#{s.amqp_url}/upstream", nil, "")

        with_channel(s) do |ch|
          ch.queue("federated_q")
          link = upstream.link(vhost_downstream.queues["federated_q"])
          link.name.should eq "federated_q"
          wait_for { link.state.running? }
          vhost_upstream.queues.has_key?("federated_q").should be_true
        end
      end
    end

    it "should federate queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream", s.amqp_url, nil, "federation_q1")

        with_channel(s) do |ch|
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
    end

    it "should not federate queue if no downstream consumer" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream wo downstream", s.amqp_url, nil, "federation_q1")

        with_channel(s) do |ch|
          x = UpstreamSpecHelpers.setup_qs(ch).first
          x.publish "federate me", "federation_q1"
          link = upstream.link(vhost.queues["federation_q2"])
          wait_for { link.state.running? }
          vhost.queues["federation_q1"].message_count.should eq 1
          vhost.queues["federation_q2"].message_count.should eq 0
        end
      end
    end

    it "should federate queue with ack mode no-ack" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream no-ack", s.amqp_url, nil, "federation_q1",
          ack_mode: LavinMQ::Federation::AckMode::NoAck)

        with_channel(s) do |ch|
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
    end

    it "should federate queue with ack mode on-publish" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream on-publish", s.amqp_url, nil, "federation_q1",
          ack_mode: LavinMQ::Federation::AckMode::OnPublish)

        with_channel(s) do |ch|
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
    end

    it "should reconnect queue link after upstream disconnect" do
      with_amqp_server do |s|
        upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation(s, "ef test upstream restart", nil, "upstream_q")
        UpstreamSpecHelpers.start_link(upstream, "downstream_q", "queues")
        s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
        s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

        upstream_vhost.declare_exchange("upstream_ex", "topic", true, false)
        upstream_vhost.declare_queue("upstream_q", true, false)
        downstream_vhost.declare_exchange("downstream_ex", "topic", true, false)
        downstream_vhost.declare_queue("downstream_q", true, false)

        wait_for { downstream_vhost.queues["downstream_q"].policy.try(&.name) == "FE" }
        wait_for { upstream.links.first?.try &.state.running? }
        sleep 0.1.seconds

        # Disconnect the federation link
        upstream_vhost.connections.each do |conn|
          next unless conn.client_name.starts_with?("Federation link")
          conn.close
        end
        sleep 0.1.seconds

        # wait for federation link to be reconnected
        wait_for { upstream.links.first?.try &.state.running? }
        wait_for { upstream.links.first?.try { |l| l.@upstream_connection.try &.closed? == false } }
      end
    end

    it "should resume federation after downstream reconnects" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream reconnect", s.amqp_url, nil, "federation_q1")
        msgs = [] of AMQP::Client::DeliverMessage

        with_channel(s) do |ch|
          x, q2 = UpstreamSpecHelpers.setup_qs ch
          x.publish "federate me", "federation_q1"
          upstream.link(vhost.queues["federation_q2"])
          q2.subscribe do |msg|
            msgs << msg
          end
          wait_for { msgs.size == 1 }
          msgs.size.should eq 1
        end

        with_channel(s) do |ch|
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
    end

    it "should keep message properties" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream props", s.amqp_url, nil, "federation_q1")

        with_channel(s) do |ch|
          x, q2 = UpstreamSpecHelpers.setup_qs ch
          x.publish "federate me", "federation_q1", props: AMQP::Client::Properties.new(content_type: "application/json")
          upstream.link(vhost.queues["federation_q2"])
          msgs = [] of AMQP::Client::DeliverMessage
          q2.subscribe { |msg| msgs << msg }
          wait_for { msgs.size == 1 }
          msgs.first.properties.content_type.should eq "application/json"
        end
      end
    end

    it "should only transfer messages when downstream has consumer" do
      with_amqp_server do |s|
        ds_queue_name = Random::Secure.hex(10)
        us_queue_name = Random::Secure.hex(10)
        upstream, us_vhost, ds_vhost = UpstreamSpecHelpers.setup_federation(s, Random::Secure.hex(10), nil, us_queue_name)

        UpstreamSpecHelpers.start_link(upstream, ds_queue_name, "queues")
        s.users.add_permission("guest", us_vhost.name, /.*/, /.*/, /.*/)
        s.users.add_permission("guest", ds_vhost.name, /.*/, /.*/, /.*/)

        # publish 1 message
        with_channel(s, vhost: us_vhost.name) do |upstream_ch|
          upstream_q = upstream_ch.queue(us_queue_name)
          upstream_q.publish "msg1"
        end

        # consume 1 message
        with_channel(s, vhost: ds_vhost.name) do |downstream_ch|
          downstream_q = downstream_ch.queue(ds_queue_name)
          wait_for { ds_vhost.queues[ds_queue_name].policy.try(&.name) == "FE" }
          wait_for { upstream.links.first?.try &.state.running? }

          msgs = Channel(String).new
          downstream_q.subscribe do |msg|
            msgs.send msg.body_io.to_s
          end
          msgs.receive.should eq "msg1"
          wait_for { s.vhosts[ds_vhost.name].queues[ds_queue_name].message_count == 0 }
        end

        wait_for { s.vhosts[ds_vhost.name].queues[ds_queue_name].consumers.empty? }

        # publish another message
        with_channel(s, vhost: us_vhost.name) do |upstream_ch|
          upstream_q = upstream_ch.queue(us_queue_name)
          upstream_q.publish "msg2"
        end

        # resume consuming on downstream, should get 1 message
        with_channel(s, vhost: ds_vhost.name) do |downstream_ch|
          msgs = Channel(String).new
          downstream_ch.queue(ds_queue_name).subscribe do |msg|
            msgs.send msg.body_io.to_s
          end
          msgs.receive.should eq "msg2"
        end
      end
    end

    it "should stop transfering messages if downstream consumer disconnects" do
      with_amqp_server do |s|
        ds_queue_name = Random::Secure.hex(10)
        us_queue_name = Random::Secure.hex(10)
        upstream, us_vhost, ds_vhost = UpstreamSpecHelpers.setup_federation(s, Random::Secure.hex(10), nil, us_queue_name, 1_u16)
        UpstreamSpecHelpers.start_link(upstream, ds_queue_name, "queues")
        s.users.add_permission("guest", us_vhost.name, /.*/, /.*/, /.*/)
        s.users.add_permission("guest", ds_vhost.name, /.*/, /.*/, /.*/)
        message_count = 2

        ds_vhost.declare_queue(ds_queue_name, true, false)
        ds_queue = ds_vhost.queues[ds_queue_name].as(LavinMQ::AMQP::Queue)

        wait_for { ds_queue.policy.try(&.name) == "FE" }
        wait_for { upstream.links.first?.try &.state.running? }

        us_queue = us_vhost.queues[us_queue_name].as(LavinMQ::AMQP::Queue)

        # publish 2 messages to upstream queue
        with_channel(s, vhost: us_vhost.name) do |upstream_ch|
          upstream_q = upstream_ch.queue(us_queue_name)
          message_count.times do |i|
            upstream_q.publish_confirm "msg#{i}"
          end
        end

        us_queue.message_count.should eq message_count

        wg = WaitGroup.new(1)
        spawn do
          # Wait for queue to receive one consumer (subscribe)
          ds_queue.@consumers_empty.when_false.receive
          # Wait for queue to lose consumer (unsubscribe)
          ds_queue.@consumers_empty.when_true.receive
          wg.done
        end
        Fiber.yield # yield so our receive? above is called

        # consume 1 message from downstream queue
        with_channel(s, vhost: ds_vhost.name) do |downstream_ch|
          downstream_ch.prefetch(1)
          downstream_q = downstream_ch.queue(ds_queue_name)
          downstream_q.subscribe(tag: "c", no_ack: false, block: true) do |msg|
            Fiber.yield # to let the sync fiber above run to call receive? a second time
            msg.ack
            downstream_q.unsubscribe("c")
          end
        end

        wg.wait
        Fiber.yield # let things happen?

        # One message has been transferred?
        us_queue.message_count.should eq 1

        # resume consuming on downstream, federation should start again
        with_channel(s, vhost: ds_vhost.name) do |downstream_ch|
          ch = Channel(Nil).new
          downstream_q = downstream_ch.queue(ds_queue_name)
          downstream_q.subscribe(tag: "c2", no_ack: false) do |msg|
            msg.ack
            downstream_q.unsubscribe("c2")
            ch.close
          end

          select
          when ch.receive?
          when timeout 3.seconds
            fail "federation didn't resume? timeout waiting for message on downstream queue"
          end

          us_queue.message_count.should eq 0
          ds_queue.message_count.should eq 0
        end
      end
    end

    describe "when @queue is nil" do
      describe "#link(Queue)" do
        it "should create link that consumes upstream queue with same name as downstream queue" do
          with_amqp_server do |s|
            vhost = s.vhosts["/"]

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
  end

  describe "Exchange federation" do
    it "should federate exchange" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "ef test upstream", s.amqp_url, "upstream_ex")

        with_channel(s) do |ch|
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
    end

    it "should federate exchange even with no downstream consumer" do
      with_amqp_server do |s|
        upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation(s, "ef test upstream wo downstream", "upstream_ex")
        UpstreamSpecHelpers.start_link(upstream)
        s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
        s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)
        with_channel(s, vhost: "upstream") do |upstream_ch|
          with_channel(s, vhost: "downstream") do |downstream_ch|
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
    end

    it "should continue after upstream restart" do
      with_amqp_server do |s|
        upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_federation(s, "ef test upstream restart", "upstream_ex")
        UpstreamSpecHelpers.start_link(upstream)
        s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
        s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

        with_channel(s, vhost: "upstream") do |upstream_ch|
          with_channel(s, vhost: "downstream") do |downstream_ch|
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
            sleep 10.milliseconds # allow the downstream federation to ack the msg
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
      end
    end

    it "should delete internal exchange and queue after deleting a federation link" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream, upstream_vhost = UpstreamSpecHelpers.setup_federation(s, "qf test upstream delete", "upstream_ex", "upstream_q")
        federation_name = "federation: upstream_ex -> #{System.hostname}:downstream:downstream_ex"

        with_channel(s) do |ch|
          downstream_ex = ch.exchange("downstream_ex", "topic")
          link = upstream.link(vhost.exchanges[downstream_ex.name])
          wait_for { link.state.running? }
          upstream_vhost.queues.has_key?(federation_name).should eq true
          upstream_vhost.exchanges.has_key?(federation_name).should eq true
          link.terminate
          upstream_vhost.queues.has_key?(federation_name).should eq false
          upstream_vhost.exchanges.has_key?(federation_name).should eq false
        end
      end
    end

    {% for descr, v in {nil: nil, empty: ""} %}
    describe "when @exchange is {{descr}}" do
      it "should use downstream exchange name as upstream exchange" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]

          vhost.declare_exchange("ex1", "topic", true, false)

          upstream = LavinMQ::Federation::Upstream.new(vhost, "test", "amqp://", {{v}})
          link1 = upstream.link(vhost.exchanges["ex1"])

          link1.@upstream_exchange.should eq "ex1"
        end
      end
    end
  {% end %}
  end

  describe "QueueLink" do
    it "set x-received-from" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        upstream = LavinMQ::Federation::Upstream.new(vhost, "qf x-received-from", s.amqp_url, exchange: nil, queue: "federation_q1")

        with_channel(s) do |ch|
          x, q2 = UpstreamSpecHelpers.setup_qs ch
          x.publish "foo", "federation_q1"
          upstream.link(vhost.queues["federation_q2"])
          msgs = [] of AMQP::Client::DeliverMessage
          wg = WaitGroup.new(1)
          q2.subscribe do |msg|
            msgs << msg
            wg.done
          end
          wg.wait
          headers = msgs.first.properties.headers.should_not be_nil
          headers["x-received-from"].as(Array(AMQ::Protocol::Field)).should_not be_nil
        end
      end
    end

    it "append to x-received-from" do
      # Sets up a "federation chain"
      with_amqp_server do |s|
        vhost1 = s.vhosts.create("one")
        vhost2 = s.vhosts.create("two")
        vhost3 = s.vhosts.create("three")

        vhost1.declare_queue("q1", durable: true, auto_delete: false)
        vhost2.declare_queue("q2", durable: true, auto_delete: false)
        vhost3.declare_queue("q3", durable: true, auto_delete: false)

        vhost1.queues["q1"]
        q2 = vhost2.queues["q2"]
        q3 = vhost3.queues["q3"]

        url = URI.parse(s.amqp_url)

        vhost1_url = url.dup
        vhost1_url.path = vhost1.name
        upstream_q1_to_q2 = LavinMQ::Federation::Upstream.new(
          vhost2, "upstream q1 to q2", vhost1_url.to_s,
          exchange: nil, queue: "q1", max_hops: 100i64)
        link_q2 = upstream_q1_to_q2.link(q2)

        vhost2_url = url.dup
        vhost2_url.path = vhost2.name
        upstream_q2_to_q3 = LavinMQ::Federation::Upstream.new(
          vhost3, "upstream q2 to q3", vhost2_url.to_s,
          exchange: nil, queue: "q2", max_hops: 100i64)
        link_q3 = upstream_q2_to_q3.link(q3)

        wait_for { link_q2.state.running? && link_q3.state.running? }

        with_channel(s, vhost: "three") do |ch|
          ch_q3 = ch.queue("q3")

          wg = WaitGroup.new(1)
          ch_q3.subscribe do |msg|
            wg.done
            headers = msg.properties.headers.should_not be_nil
            x_received_from = headers["x-received-from"].should be_a Array(AMQ::Protocol::Field)
            x_received_from.size.should eq 2
          end

          with_channel(s, vhost: "one") do |ch_pub|
            ch_pub.queue("q1").publish "foo"
          end

          wg.wait
        end
      end
    end
  end

  describe "ExchangeLink" do
    it "should reflect all bindings to upstream exchange" do
      with_amqp_server do |s|
        upstream, upstream_vhost, _ = UpstreamSpecHelpers.setup_federation(s, "ef test bindings", "upstream_ex")
        with_channel(s, vhost: "downstream") do |downstream_ch|
          downstream_ch.exchange("downstream_ex", "topic")
          queues = [] of AMQP::Client::Queue
          10.times do |i|
            downstream_q = downstream_ch.queue("")
            downstream_q.bind("downstream_ex", "before.link.#{i}")
            queues << downstream_q
          end

          UpstreamSpecHelpers.start_link(upstream)
          wait_for { upstream.links.first?.try &.state.running? }

          upstream_ex = upstream_vhost.exchanges["upstream_ex"].as(LavinMQ::AMQP::Exchange)
          upstream_ex.bindings_details.size.should eq queues.size

          10.times do |i|
            downstream_q = downstream_ch.queue("")
            downstream_q.bind("downstream_ex", "after.link.#{i}")
            queues << downstream_q
          end

          wait_for(timeout: 10.milliseconds) { upstream_ex.bindings_details.size == queues.size }
          queues.each &.delete
          wait_for(timeout: 10.milliseconds) { upstream_ex.bindings_details.empty? }
        end
      end
    end

    it "set x-received-from" do
      with_amqp_server do |s|
        vhost1 = s.vhosts.create("one")
        vhost2 = s.vhosts.create("two")

        vhost1.declare_exchange("upstream_ex", "topic", durable: true, auto_delete: false)
        vhost2.declare_exchange("downstream_ex", "topic", durable: true, auto_delete: false)
        vhost2.declare_queue("downstream_q", durable: true, auto_delete: false)

        downstream_ex = vhost2.exchanges["downstream_ex"]
        downstream_q = vhost2.queues["downstream_q"]
        downstream_ex.bind(downstream_q, "#")

        url = URI.parse(s.amqp_url)
        url.path = vhost1.name
        upstream = LavinMQ::Federation::Upstream.new(vhost2, "ef x-received-from", url.to_s, exchange: "upstream_ex", queue: nil)

        with_channel(s, vhost: "two") do |ch|
          link = upstream.link(downstream_ex)

          wait_for { link.state.running? }

          wg = WaitGroup.new(1)
          q = ch.queue("downstream_q", passive: true)
          q.subscribe(tag: "downstream_q_consumer") do |msg|
            headers = msg.properties.headers.should_not be_nil
            headers["x-received-from"].as(Array(AMQ::Protocol::Field)).should_not be_nil
            wg.done
          end

          with_channel(s, vhost: "one") do |ch_pub|
            ch_pub.exchange("upstream_ex", "topic").publish "foo", "routing.key"
          end

          wg.wait
        end
      end
    end

    it "append to x-received-from" do
      # Sets up a "federation chain"
      with_amqp_server do |s|
        vhost1 = s.vhosts.create("one")
        vhost2 = s.vhosts.create("two")
        vhost3 = s.vhosts.create("three")

        vhost1.declare_exchange("ex1", "topic", durable: true, auto_delete: false)
        vhost2.declare_exchange("ex2", "topic", durable: true, auto_delete: false)
        vhost3.declare_exchange("ex3", "topic", durable: true, auto_delete: false)

        ex1 = vhost1.exchanges["ex1"]
        ex2 = vhost2.exchanges["ex2"]
        ex3 = vhost3.exchanges["ex3"]

        vhost3.declare_queue("q3", durable: true, auto_delete: false)
        downstream_q = vhost3.queues["q3"]
        downstream_ex = vhost3.exchanges["ex3"]

        url = URI.parse(s.amqp_url)

        vhost1_url = url.dup
        vhost1_url.path = vhost1.name
        upstream_ex1_to_ex2 = LavinMQ::Federation::Upstream.new(
          vhost2, "upstream ex1 to ex2", vhost1_url.to_s,
          exchange: "ex1", queue: nil, max_hops: 100i64)

        vhost2_url = url.dup
        vhost2_url.path = vhost2.name
        upstream_ex2_to_ex3 = LavinMQ::Federation::Upstream.new(
          vhost3, "upstream ex2 to ex3", vhost2_url.to_s,
          exchange: "ex2", queue: nil, max_hops: 100i64)

        link_ex3 = upstream_ex2_to_ex3.link(ex3)
        link_ex2 = upstream_ex1_to_ex2.link(ex2)
        wait_for { link_ex2.state.running? && link_ex3.state.running? }

        downstream_ex.bind(downstream_q, "#")
        with_channel(s, vhost: "three") do |ch|
          ch_q3 = ch.queue("q3")

          wg = WaitGroup.new(1)
          ch_q3.subscribe do |msg|
            wg.done
            headers = msg.properties.headers.should_not be_nil
            x_received_from = headers["x-received-from"].should be_a Array(AMQ::Protocol::Field)
            x_received_from.size.should eq 2
          end

          with_channel(s, vhost: "one") do |ch_pub|
            ch_pub.exchange("ex1", "topic").publish "foo", "routing.key"
          end

          wg.wait
        end
      end
    end

    it "set x-bound-from" do
      with_http_server do |_http, s|
        vhost1 = s.vhosts.create("one")
        vhost2 = s.vhosts.create("two")

        vhost1.declare_exchange("upstream_ex", "topic", durable: true, auto_delete: false)
        vhost2.declare_exchange("downstream_ex", "topic", durable: true, auto_delete: false)
        vhost2.declare_queue("downstream_q", durable: true, auto_delete: false)

        downstream_ex = vhost2.exchanges["downstream_ex"]
        downstream_q = vhost2.queues["downstream_q"]
        downstream_ex.bind(downstream_q, "federation.link.rk")

        upstream_ex = vhost1.exchanges["upstream_ex"]

        url = URI.parse(s.amqp_url)
        url.path = vhost1.name
        upstream = LavinMQ::Federation::Upstream.new(vhost2, "ef x-bound-from", url.to_s, exchange: "upstream_ex", queue: nil)

        link = upstream.link(downstream_ex)

        wait_for { link.state.running? }
        wait_for { upstream_ex.bindings_details.size == 1 }

        binding_details = upstream_ex.bindings_details.find { |x| x.routing_key == "federation.link.rk" }.should_not be_nil
        arguments = binding_details.arguments.should_not be_nil
        x_bound_from = arguments["x-bound-from"].should be_a Array(AMQ::Protocol::Field)
        x_bound_from.size.should eq 1
      end
    end

    describe "exchange federation chain" do
      it "append to x-bound-from" do
        with_http_server do |_http, s|
          UpstreamSpecHelpers.create_fe_chain(s, chain_length: 10)
          downstream_vhost = s.vhosts["v10"]
          downstream_ex = downstream_vhost.exchanges["fe"]
          downstream_vhost.declare_queue("q", durable: true, auto_delete: false)
          q = downstream_vhost.queues["q"]
          # This binding should be propagated all the way to "fe" in "v1"
          # For each hop x-bound-from should be extended with one new entry
          downstream_ex.bind(q, "spec.routing.key")

          # Verify bindings on exchange "fe" from vhost v1 to
          # vhost v9.
          1.to(9) do |i|
            fe = s.vhosts["v#{i}"].exchanges["fe"]
            fe.bindings_details.size.should eq 1
            fe_bk = fe.bindings_details.first.binding_key
            args = fe_bk.arguments.should_not be_nil
            x_bound_from = args["x-bound-from"].should be_a(Array(AMQ::Protocol::Field))
            # in v1 there should be 9 entries
            # in v2 there should be 8 entries etc
            x_bound_from.size.should eq(9 - i + 1)
            first_bound_from = x_bound_from.first.should be_a(AMQ::Protocol::Table)
            # The first (last appended) x-bound-from should be the previous vhost (v2 is prev to v1 etc)
            first_bound_from["vhost"].should eq "v#{i + 1}"
            # The first (last appended) x-bound-from should always be the downstream (v10)
            last_bound_from = x_bound_from.last.should be_a(AMQ::Protocol::Table)
            last_bound_from["vhost"].should eq "v10"
          end
        end
      end

      it "respects lower maxhops" do
        with_http_server do |_http, s|
          _, upstreams = UpstreamSpecHelpers.create_fe_chain(s, chain_length: 10)

          # By setting max_hops to 1 on v5, bindings should reach v4 but no further
          us = upstreams.find { |u| u.vhost.name == "v5" }.should_not be_nil
          us.max_hops = 1

          downstream_vhost = s.vhosts["v10"]
          downstream_ex = downstream_vhost.exchanges["fe"]
          downstream_vhost.declare_queue("q", durable: true, auto_delete: false)
          q = downstream_vhost.queues["q"]
          # This binding should only be propagated to "fe" in "v4", because
          # max_hops on v5 is 1.
          downstream_ex.bind(q, "spec.routing.key")

          # Verify bindings on exchange "fe" from vhost v1 to
          # vhost v4.
          1.to(3) do |i|
            fe = s.vhosts["v#{i}"].exchanges["fe"]
            fe.bindings_details.size.should eq 0
          end

          # Verify bindings on exchange "fe" from vhost v4 to
          # vhost v9.
          4.to(9) do |i|
            fe = s.vhosts["v#{i}"].exchanges["fe"]
            fe.bindings_details.size.should eq 1
            fe_bk = fe.bindings_details.first.binding_key
            args = fe_bk.arguments.should_not be_nil
            x_bound_from = args["x-bound-from"].should be_a(Array(AMQ::Protocol::Field))
            # in v4 there should be 6 entries
            # in v5 there should be 5 entries etc
            x_bound_from.size.should eq(9 - i + 1), "Expected #{6 - i + 1} entries in v#{i} but got #{x_bound_from.size}"
            first_bound_from = x_bound_from.first.should be_a(AMQ::Protocol::Table)
            # The first (last appended) x-bound-from should be the previous vhost (v2 is prev to v1 etc)
            first_bound_from["vhost"].should eq "v#{i + 1}"
            # The first (last appended) x-bound-from should always be the downstream (v10)
            last_bound_from = x_bound_from.last.should be_a(AMQ::Protocol::Table)
            last_bound_from["vhost"].should eq "v10"
          end
        end
      end
    end
  end
end
