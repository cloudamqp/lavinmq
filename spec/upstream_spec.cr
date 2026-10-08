require "./spec_helper"
require "../src/lavinmq/federation/upstream"
require "../src/lavinmq/federation/upstream_store"

module FederationSpecHelpers
  # Every scenario runs with an upstream in this broker, reached in-process (a
  # URI without host), and with one reached over AMQP (a URI with host).
  KINDS = {"in-process", "remote"}

  def self.uri(s : LavinMQ::Server, kind : String, vhost : String) : String
    case kind
    when "in-process" then "amqp:///#{vhost}"
    else                   "#{s.amqp_server.url}/#{vhost}"
    end
  end

  # Creates vhosts "upstream" and "downstream" and an upstream named "up" in
  # the downstream vhost, the way the HTTP API and definitions do
  def self.setup(s, kind, **config)
    upstream_vhost = s.vhosts.create("upstream")
    downstream_vhost = s.vhosts.create("downstream")
    params = {"uri" => uri(s, kind, "upstream"), "reconnect-delay" => 1}.merge(config.to_h.transform_keys(&.to_s))
    downstream_vhost.add_parameter(LavinMQ::Parameter.new("federation-upstream", "up", JSON.parse(params.to_json)))
    {upstream_vhost, downstream_vhost}
  end

  def self.federate(vhost : LavinMQ::VHost, pattern : String, apply_to : String, upstream = "up")
    definition = {"federation-upstream" => JSON::Any.new(upstream)}
    vhost.add_policy("federation", pattern, apply_to, definition, 0_i8)
  end

  def self.link(vhost : LavinMQ::VHost, upstream = "up") : LavinMQ::Federation::Upstream::Link
    wait_for { vhost.upstreams.find { |u| u.name == upstream }.try(&.links.first?) }
  end

  # Applying policies can replace a link, so look it up until one runs
  def self.running_link(vhost, upstream = "up") : LavinMQ::Federation::Upstream::Link
    wait_for do
      link = vhost.upstreams.find { |u| u.name == upstream }.try(&.links.first?)
      link if link && link.state.running?
    end
  end

  def self.publish(vhost : LavinMQ::VHost, exchange : String, routing_key : String, body : String)
    vhost.publish(LavinMQ::Message.new(exchange, routing_key, body))
  end

  def self.bodies(vhost : LavinMQ::VHost, queue : String) : Array(String)
    q = vhost.queue(queue)
    bodies = [] of String
    while q.basic_get(true) { |env| bodies << String.new(env.message.body) }
    end
    bodies
  end

  # Upstream x-federation-upstream exchange the link created
  def self.upstream_link_exchange(vhost : LavinMQ::VHost)
    vhost.exchanges.find { |ex| ex.type == "x-federation-upstream" }
  end

  # A proxy to the server that holds each connection until `gate` closes,
  # to stop a link while it is connecting
  def self.with_gated_proxy(s, &)
    target = URI.parse(s.amqp_server.url)
    proxy = TCPServer.new("127.0.0.1", 0)
    accepted = Channel(Nil).new(1)
    gate = Channel(Nil).new
    spawn(name: "gated proxy") do
      while client = proxy.accept?
        select
        when accepted.send nil
        else
        end
        gate.receive?
        server = TCPSocket.new(target.hostname.not_nil!, target.port.not_nil!)
        spawn { IO.copy(client, server) rescue nil; server.close rescue nil }
        spawn { IO.copy(server, client) rescue nil; client.close rescue nil }
      end
    end
    url = target.dup
    url.host = "127.0.0.1"
    url.port = proxy.local_address.port
    yield url, accepted, gate
  ensure
    gate.try &.close
    proxy.try &.close
  end
end

describe LavinMQ::Federation do
  FederationSpecHelpers::KINDS.each do |kind|
    describe "exchange federation with an #{kind} upstream" do
      it "federates messages published upstream" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_exchange("fx", "topic", true, false)
          down.declare_exchange("fx", "topic", true, false)
          down.declare_queue("fq", true, false)
          down.bind_queue("fq", "fx", "a.#")
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          FederationSpecHelpers.publish(up, "fx", "a.b", "match")
          FederationSpecHelpers.publish(up, "fx", "b.a", "no match")
          should_eventually(eq 1) { down.queue("fq").message_count }
          down.queue("fq").basic_get(true) do |env|
            String.new(env.message.body).should eq "match"
            env.message.routing_key.should eq "a.b"
            hops = env.message.properties.headers.not_nil!["x-received-from"].as(Array)
            hop = hops.first.as(AMQ::Protocol::Table)
            hop["uri"].should eq FederationSpecHelpers.uri(s, kind, "upstream")
            hop["exchange"].should eq "fx"
            hop["redelivered"].should be_false
          end
        end
      end

      it "mirrors downstream bindings, including later ones, to the upstream" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          down.declare_exchange("fx", "direct", true, false)
          down.declare_queue("fq", true, false)
          down.bind_queue("fq", "fx", "early")
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          link_ex = FederationSpecHelpers.upstream_link_exchange(up).not_nil!
          should_eventually(eq ["early"]) do
            up.exchange("fx").bindings_details.select(&.destination.==(link_ex)).map(&.binding_key.routing_key)
          end
          down.bind_queue("fq", "fx", "late")
          should_eventually(eq ["early", "late"]) do
            up.exchange("fx").bindings_details.select(&.destination.==(link_ex)).map(&.binding_key.routing_key).sort!
          end
          FederationSpecHelpers.publish(up, "fx", "late", "m")
          should_eventually(eq 1) { down.queue("fq").message_count }
          down.unbind_queue("fq", "fx", "early")
          should_eventually(eq ["late"]) do
            up.exchange("fx").bindings_details.select(&.destination.==(link_ex)).map(&.binding_key.routing_key)
          end
        end
      end

      it "removes its upstream queue and exchange when the federation is removed" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          down.declare_exchange("fx", "topic", true, false)
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          up.queues.size.should eq 1
          FederationSpecHelpers.upstream_link_exchange(up).should_not be_nil
          down.delete_policy("federation")
          should_eventually(eq 0) { up.queues.size }
          FederationSpecHelpers.upstream_link_exchange(up).should be_nil
        end
      end

      it "keeps its upstream queue on shutdown, and picks up where it left off" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_exchange("fx", "topic", true, false)
          down.declare_exchange("fx", "topic", true, false)
          down.declare_queue("fq", true, false)
          down.bind_queue("fq", "fx", "#")
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          down.upstreams.stop_all # what a shutdown does
          up.queues.size.should eq 1
          FederationSpecHelpers.publish(up, "fx", "k", "while down")
          up.queues.first.message_count.should eq 1
          down.upstreams.link("up", down.exchange("fx"))
          should_eventually(eq 1) { down.queue("fq").message_count }
        end
      end

      it "reports the link in the HTTP API without credentials" do
        with_http_server do |http, s|
          _up, down = FederationSpecHelpers.setup(s, kind)
          down.declare_exchange("fx", "topic", true, false)
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          response = http.get("/api/federation-links/downstream")
          response.status_code.should eq 200
          link = JSON.parse(response.body)[0]
          link["upstream"].should eq "up"
          link["type"].should eq "exchange"
          link["resource"].should eq "fx"
          link["status"].should eq "running"
          link["uri"].should eq FederationSpecHelpers.uri(s, kind, "upstream")
          link["consumer-tag"].should eq "federation-link-up"
        end
      end

      it "uses the configured consumer tag" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind, "consumer-tag": "my-tag")
          down.declare_exchange("fx", "topic", true, false)
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          q = up.queues.first
          should_eventually(eq "my-tag") { JSON.parse(q.to_json)["consumer_details"][0]?.try &.["consumer_tag"] }
        end
      end

      it "doesn't lose messages a full downstream queue refuses" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_exchange("fx", "fanout", true, false)
          down.declare_exchange("fx", "fanout", true, false)
          args = AMQ::Protocol::Table.new({"x-max-length" => 2_i64, "x-overflow" => "reject-publish"})
          down.declare_queue("fq", true, false, args)
          down.bind_queue("fq", "fx", "")
          FederationSpecHelpers.federate(down, "^fx$", "exchanges")
          FederationSpecHelpers.running_link(down)
          5.times { |i| FederationSpecHelpers.publish(up, "fx", "", "m#{i}") }
          should_eventually(eq 2) { down.queue("fq").message_count }
          upstream_q = up.queues.first
          should_eventually(eq 3) { upstream_q.message_count + upstream_q.unacked_count }
          # Draining the downstream queue lets the rest through
          received = [] of String
          should_eventually(eq 5) { received.concat(FederationSpecHelpers.bodies(down, "fq")).size }
          received.sort.should eq ["m0", "m1", "m2", "m3", "m4"]
        end
      end
    end

    describe "queue federation with an #{kind} upstream" do
      it "moves messages only while the downstream queue has consumers" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_queue("fq", true, false)
          down.declare_queue("fq", true, false)
          3.times { |i| FederationSpecHelpers.publish(up, "", "fq", "m#{i}") }
          FederationSpecHelpers.federate(down, "^fq$", "queues")
          FederationSpecHelpers.running_link(down)
          sleep 50.milliseconds
          up.queue("fq").message_count.should eq 3

          received = Channel(String).new(10)
          with_channel(s, vhost: "downstream") do |ch|
            ch.prefetch(10)
            ch.queue("fq", passive: true).subscribe(no_ack: false) do |msg|
              hop = msg.properties.headers.not_nil!["x-received-from"].as(Array).first.as(AMQ::Protocol::Table)
              hop["queue"].should eq "fq"
              msg.ack
              received.send msg.body_io.to_s
            end
            Array.new(3) { received.receive }.should eq ["m0", "m1", "m2"]
          end
          # The consumer is gone: messages stay upstream
          should_eventually(eq 0) { down.queue("fq").consumer_count }
          sleep 50.milliseconds
          FederationSpecHelpers.publish(up, "", "fq", "after")
          sleep 50.milliseconds
          up.queue("fq").message_count.should eq 1
          should_eventually(eq 0) { up.queue("fq").unacked_count }
          down.queue("fq").message_count.should eq 0
        end
      end

      it "uses the configured upstream queue name" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind, queue: "other")
          up.declare_queue("other", true, false)
          down.declare_queue("fq", true, false)
          FederationSpecHelpers.publish(up, "", "other", "m")
          FederationSpecHelpers.federate(down, "^fq$", "queues")
          FederationSpecHelpers.running_link(down)
          with_channel(s, vhost: "downstream") do |ch|
            received = Channel(String).new(1)
            ch.queue("fq", passive: true).subscribe { |msg| received.send msg.body_io.to_s }
            received.receive.should eq "m"
          end
        end
      end

      {% for mode in %w[on-confirm on-publish no-ack] %}
        it "moves messages with ack mode {{ mode.id }}" do
          with_amqp_server do |s|
            up, down = FederationSpecHelpers.setup(s, kind, "ack-mode": {{ mode }}, "prefetch-count": 5)
            up.declare_queue("fq", true, false)
            down.declare_queue("fq", true, false)
            20.times { |i| FederationSpecHelpers.publish(up, "", "fq", "m#{i}") }
            FederationSpecHelpers.federate(down, "^fq$", "queues")
            FederationSpecHelpers.running_link(down)
            count = Atomic(Int32).new(0)
            with_channel(s, vhost: "downstream") do |ch|
              ch.prefetch(100)
              ch.queue("fq", passive: true).subscribe(no_ack: true) { count.add(1) }
              should_eventually(eq 20) { count.get }
            end
            should_eventually(eq 0) { up.queue("fq").message_count + up.queue("fq").unacked_count }
          end
        end
      {% end %}

      it "moves every message, once, to a consumer at its prefetch limit" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_queue("fq", true, false)
          down.declare_queue("fq", true, false)
          10.times { |i| FederationSpecHelpers.publish(up, "", "fq", "m#{i}") }
          FederationSpecHelpers.federate(down, "^fq$", "queues")
          FederationSpecHelpers.running_link(down)
          received = [] of String
          with_channel(s, vhost: "downstream") do |ch|
            ch.prefetch(1)
            ch.queue("fq", passive: true).subscribe(no_ack: false) do |msg|
              sleep 1.millisecond # a slow consumer
              received << msg.body_io.to_s
              msg.ack
            end
            should_eventually(eq 10) { received.size }
          end
          received.sort.should eq Array.new(10) { |i| "m#{i}" }.sort
          should_eventually(eq 0) { up.queue("fq").message_count + up.queue("fq").unacked_count }
          down.queue("fq").message_count.should eq 0
        end
      end

      it "stops when the federated queue is deleted" do
        with_amqp_server do |s|
          up, down = FederationSpecHelpers.setup(s, kind)
          up.declare_queue("fq", true, false)
          down.declare_queue("fq", true, false)
          FederationSpecHelpers.federate(down, "^fq$", "queues")
          link = FederationSpecHelpers.running_link(down)
          down.delete_queue("fq")
          should_eventually(be_true) { link.state.terminated? }
          down.upstreams.first.links.should be_empty
        end
      end
    end
  end

  describe "link lifecycle" do
    it "stops the link when the federated queue is deleted or closed" do
      with_amqp_server do |s|
        _up, down = FederationSpecHelpers.setup(s, "in-process")
        upstream = down.upstreams.find! { |u| u.name == "up" }
        {:delete, :close}.each do |how|
          down.declare_queue("lq", true, false)
          q = down.queue("lq")
          link = upstream.link(q)
          how == :delete ? q.delete : q.close
          upstream.links.should be_empty
          wait_for { link.state.terminated? }
          down.delete_queue("lq")
        end
      end
    end

    it "stops the link before the closing queue is deleted" do
      with_amqp_server do |s|
        _up, down = FederationSpecHelpers.setup(s, "in-process")
        upstream = down.upstreams.find! { |u| u.name == "up" }
        # Transient, so closing deletes it and frees the name for a redeclare
        down.declare_queue("lq", false, false)
        q = down.queue("lq")
        upstream.link(q)
        closed = Channel(Nil).new
        q.@msg_store_lock.synchronize do
          spawn { q.close; closed.close }
          # close is now parked on the lock, before it deletes the queue
          wait_for { q.closed? }
          upstream.links.should be_empty
        end
        closed.receive?
        down.queue?("lq").should be_nil
      end
    end

    it "keeps the link of a queue or exchange redeclared under the same name" do
      with_amqp_server do |s|
        _up, down = FederationSpecHelpers.setup(s, "in-process")
        upstream = down.upstreams.find! { |u| u.name == "up" }
        down.declare_queue("lq", false, false)
        old_q = down.queue("lq")
        old_q.delete
        down.declare_queue("lq", false, false)
        q_link = upstream.link(down.queue("lq"))
        down.declare_exchange("lx", "topic", true, false)
        old_ex = down.exchange("lx").as(LavinMQ::AMQP::Exchange)
        down.delete_exchange("lx")
        down.declare_exchange("lx", "topic", true, false)
        ex_link = upstream.link(down.exchange("lx").as(LavinMQ::AMQP::Exchange))
        # A late cleanup for the old queue or exchange must not take the new one's link
        upstream.stop_link(old_q)
        upstream.stop_link(old_ex)
        upstream.links.should contain q_link
        upstream.links.should contain ex_link
      end
    end

    it "stops the link when the federated exchange is deleted" do
      with_amqp_server do |s|
        _up, down = FederationSpecHelpers.setup(s, "in-process")
        upstream = down.upstreams.find! { |u| u.name == "up" }
        down.declare_exchange("lx", "topic", true, false)
        link = upstream.link(down.exchange("lx").as(LavinMQ::AMQP::Exchange))
        down.delete_exchange("lx")
        upstream.links.should be_empty
        wait_for { link.state.terminated? }
      end
    end

    it "stops links of set entries with overrides when the policy is removed or the resource deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        store = vhost.upstreams
        store.create_upstream("a", JSON.parse(%({"uri": "amqp:///"})))
        store.create_upstream_set("set1", JSON.parse(%([{"upstream": "a", "prefetch-count": 99}])))
        member = store.get_set("set1").first
        vhost.declare_queue("q", false, false)
        q = vhost.queue("q")
        member.link(q)
        store.stop_link(q) # what removing the policy does
        member.links.should be_empty
        link = member.link(q)
        q.delete
        member.links.should be_empty
        wait_for { link.state.terminated? }
        vhost.declare_exchange("ex", "topic", false, false)
        member.link(vhost.exchange("ex"))
        vhost.delete_exchange("ex")
        member.links.should be_empty
      ensure
        store.try &.stop_all
      end
    end

    it "stops the links of a set entry with overrides when the set is replaced or deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        store = vhost.upstreams
        store.create_upstream("a", JSON.parse(%({"uri": "amqp:///"})))
        set_config = JSON.parse(%([{"upstream": "a", "prefetch-count": 99}]))
        vhost.declare_exchange("ex", "topic", false, false)
        ex = vhost.exchange("ex")
        store.create_upstream_set("set1", set_config)
        replaced = store.get_set("set1").first
        replaced_link = replaced.link(ex)
        store.create_upstream_set("set1", set_config)
        replaced.links.should be_empty
        wait_for { replaced_link.state.terminated? }
        deleted = store.get_set("set1").first
        deleted_link = deleted.link(ex)
        store.delete_upstream_set("set1")
        deleted.links.should be_empty
        wait_for { deleted_link.state.terminated? }
      ensure
        store.try &.stop_all
      end
    end
    {"queue", "exchange"}.each do |type|
      it "doesn't set up a #{type} link stopped while connecting" do
        with_amqp_server do |s|
          up = s.vhosts.create("upstream")
          down = s.vhosts.create("downstream")
          FederationSpecHelpers.with_gated_proxy(s) do |url, accepted, gate|
            params = {"uri" => "#{url}/upstream", "queue" => "uq", "exchange" => "ux"}
            down.add_parameter(LavinMQ::Parameter.new("federation-upstream", "up", JSON.parse(params.to_json)))
            upstream = down.upstreams.find! { |u| u.name == "up" }
            link =
              if type == "queue"
                down.declare_queue("lq", true, false)
                upstream.link(down.queue("lq"))
              else
                down.declare_exchange("lx", "topic", true, false)
                upstream.link(down.exchange("lx").as(LavinMQ::AMQP::Exchange))
              end
            accepted.receive # the link is parked in its upstream connect
            upstream.stop_link(down.queue?("lq") || down.exchange("lx").as(LavinMQ::AMQP::Exchange))
            upstream.links.should be_empty
            gate.close # let the connect complete
            wait_for { link.state.terminated? }
            # The stopped link must not have declared anything upstream
            up.queues.should be_empty
            up.exchange?("ux").should be_nil
          end
        end
      end
    end
  end

  describe "with an upstream over AMQP" do
    it "mirrors bindings made after the link reconnects" do
      with_amqp_server do |s|
        up, down = FederationSpecHelpers.setup(s, "remote")
        up.declare_exchange("fx", "topic", true, false)
        down.declare_exchange("fx", "topic", true, false)
        down.declare_queue("fq", true, false)
        FederationSpecHelpers.federate(down, "^fx$", "exchanges")
        link = FederationSpecHelpers.running_link(down)
        up.each_connection &.close("spec")
        should_eventually(be_true) { !link.state.running? }
        link = FederationSpecHelpers.running_link(down)
        link_ex = FederationSpecHelpers.upstream_link_exchange(up).not_nil!
        down.bind_queue("fq", "fx", "after.reconnect")
        should_eventually(be_true) do
          up.exchange("fx").bindings_details.any? { |b| b.destination == link_ex && b.binding_key.routing_key == "after.reconnect" }
        end
        down.unbind_queue("fq", "fx", "after.reconnect")
        should_eventually(be_false) do
          up.exchange("fx").bindings_details.any? { |b| b.destination == link_ex && b.binding_key.routing_key == "after.reconnect" }
        end
      end
    end

    it "reconnects after losing its connection" do
      with_amqp_server do |s|
        up, down = FederationSpecHelpers.setup(s, "remote")
        up.declare_exchange("fx", "topic", true, false)
        down.declare_exchange("fx", "topic", true, false)
        down.declare_queue("fq", true, false)
        down.bind_queue("fq", "fx", "#")
        FederationSpecHelpers.federate(down, "^fx$", "exchanges")
        link = FederationSpecHelpers.running_link(down)
        up.each_connection &.close("spec")
        should_eventually(be_true) { !link.state.running? }
        should_eventually(be_true, 5.seconds) { link.state.running? }
        FederationSpecHelpers.publish(up, "fx", "k", "after reconnect")
        should_eventually(eq 1) { down.queue("fq").message_count }
      end
    end
  end

  describe "with an in-process upstream" do
    it "doesn't open any AMQP connection" do
      with_amqp_server do |s|
        up, down = FederationSpecHelpers.setup(s, "in-process")
        down.declare_exchange("fx", "topic", true, false)
        FederationSpecHelpers.federate(down, "^fx$", "exchanges")
        FederationSpecHelpers.running_link(down)
        up.connections_size.should eq 0
        down.connections_size.should eq 0
      end
    end

    it "waits without spinning for a downstream consumer whose flow is off" do
      with_amqp_server do |s|
        up, down = FederationSpecHelpers.setup(s, "in-process")
        up.declare_queue("fq", true, false)
        down.declare_queue("fq", true, false)
        FederationSpecHelpers.publish(up, "", "fq", "m")
        FederationSpecHelpers.federate(down, "^fq$", "queues")
        FederationSpecHelpers.running_link(down)
        received = Channel(String).new(1)
        with_channel(s, vhost: "downstream") do |ch|
          # The consumer has prefetch room but doesn't accept: the link must
          # wait for the flow, not for the (already true) prefetch capacity
          ch.flow(false)
          ch.queue("fq", passive: true).subscribe(no_ack: true) { |msg| received.send msg.body_io.to_s }
          sleep 200.milliseconds # the link has the message, and nowhere to deliver it
          before = Process.times
          sleep 500.milliseconds
          after = Process.times
          cpu = (after.utime + after.stime) - (before.utime + before.stime)
          cpu.should be < 0.25 # a spinning link burns a whole core
          ch.flow(true)
          select
          when body = received.receive
            body.should eq "m"
          when timeout(5.seconds)
            fail "message not delivered after flow was resumed"
          end
        end
      end
    end

    it "retries until the upstream vhost exists" do
      with_amqp_server do |s|
        down = s.vhosts.create("downstream")
        params = {"uri" => "amqp:///later", "reconnect-delay" => 1}
        down.add_parameter(LavinMQ::Parameter.new("federation-upstream", "up", JSON.parse(params.to_json)))
        down.declare_exchange("fx", "topic", true, false)
        FederationSpecHelpers.federate(down, "^fx$", "exchanges")
        link = FederationSpecHelpers.link(down)
        should_eventually(be_true) { !link.error.nil? }
        s.vhosts.create("later").declare_exchange("fx", "topic", true, false)
        should_eventually(be_true, 5.seconds) { link.state.running? }
      end
    end

    it "forwards messages along a chain no further than max-hops" do
      with_amqp_server do |s|
        vhosts = (1..4).map do |i|
          v = s.vhosts.create("v#{i}")
          v.declare_exchange("fe", "topic", true, false)
          v
        end
        # v1 -> v2 -> v3 -> v4, each the upstream of the next
        vhosts.each_cons_pair do |prev, vhost|
          params = {"uri" => "amqp:///#{prev.name}", "max-hops" => 2}
          vhost.add_parameter(LavinMQ::Parameter.new("federation-upstream", "up", JSON.parse(params.to_json)))
        end
        vhosts.each { |v| v.declare_queue("q", true, false); v.bind_queue("q", "fe", "#") }
        vhosts[1..].each do |v|
          FederationSpecHelpers.federate(v, "^fe$", "exchanges")
          FederationSpecHelpers.running_link(v)
        end
        FederationSpecHelpers.publish(vhosts[0], "fe", "k", "m")
        should_eventually(eq 1) { vhosts[2].queue("q").message_count }
        vhosts[1].queue("q").message_count.should eq 1
        sleep 100.milliseconds
        vhosts[3].queue("q").message_count.should eq 0 # three hops away
        vhosts[2].queue("q").basic_get(true) do |env|
          env.message.properties.headers.not_nil!["x-received-from"].as(Array).size.should eq 2
        end
      end
    end

    it "propagates bindings no further than max-hops" do
      with_amqp_server do |s|
        vhosts = (1..3).map do |i|
          v = s.vhosts.create("b#{i}")
          v.declare_exchange("fe", "direct", true, false)
          v
        end
        vhosts.each_cons_pair do |prev, vhost|
          params = {"uri" => "amqp:///#{prev.name}", "max-hops" => 1}
          vhost.add_parameter(LavinMQ::Parameter.new("federation-upstream", "up", JSON.parse(params.to_json)))
          FederationSpecHelpers.federate(vhost, "^fe$", "exchanges")
          FederationSpecHelpers.running_link(vhost)
        end
        vhosts[2].declare_queue("q", true, false)
        vhosts[2].bind_queue("q", "fe", "key")
        # b3's binding is mirrored to b2 (one hop), but not on to b1
        should_eventually(be_true) do
          vhosts[1].exchange("fe").bindings_details.any? { |b| b.binding_key.routing_key == "key" }
        end
        bound_from = vhosts[1].exchange("fe").bindings_details.find! { |b| b.binding_key.routing_key == "key" }
          .binding_key.arguments.not_nil!["x-bound-from"].as(Array)
        bound_from.first.as(AMQ::Protocol::Table)["vhost"].should eq "b3"
        sleep 100.milliseconds
        vhosts[0].exchange("fe").bindings_details.none? { |b| b.binding_key.routing_key == "key" }.should be_true
      end
    end
  end

  describe LavinMQ::Federation::UpstreamStore do
    it "checks the user's access to an in-process upstream" do
      with_amqp_server do |s|
        s.vhosts.create("secret")
        user = s.users.create("limited", "pass")
        s.users.add_permission("limited", "/", /.*/, /.*/, /.*/)
        validate = ->(uri : String) {
          LavinMQ::Federation::UpstreamStore.validate_config!("federation-upstream",
            JSON.parse({"uri" => uri, "exchange" => "x"}.to_json), user)
        }
        validate.call("amqp:///%2f")
        expect_raises(LavinMQ::Federation::ConfigError) { validate.call("amqp:///secret") }
        # the upstream broker checks the credentials in the URI itself
        validate.call("amqp://u:p@remote/secret")
        expect_raises(LavinMQ::Federation::ConfigError) do
          LavinMQ::Federation::UpstreamStore.validate_config!("federation-upstream-set",
            JSON.parse([{"upstream" => "a", "uri" => "amqp:///secret"}].to_json), user)
        end
        # Without exchange or queue the link federates whatever a policy
        # picks, by name: restricted read or configure isn't enough
        s.users.add_permission("limited", "secret", /.*/, /^public$/, /.*/)
        expect_raises(LavinMQ::Federation::ConfigError) do
          LavinMQ::Federation::UpstreamStore.validate_config!("federation-upstream",
            JSON.parse({"uri" => "amqp:///secret"}.to_json), user)
        end
        expect_raises(LavinMQ::Federation::ConfigError) { validate.call("amqp:///secret") }
        s.users.add_permission("limited", "secret", /.*/, /.*/, /.*/)
        validate.call("amqp:///secret")
      end
    end

    it "accepts a list of URIs, like RabbitMQ, and uses the first" do
      with_http_server do |http, s|
        body = {"value" => {"uri" => ["amqp://server-name/%2f", "amqp://other/%2f"]}}.to_json
        response = http.put("/api/parameters/federation-upstream/%2f/up", body: body)
        response.status_code.should eq 201
        s.vhosts["/"].upstreams.find!(&.name.== "up").uri.host.should eq "server-name"
        body = {"value" => {"uri" => [1]}}.to_json
        response = http.put("/api/parameters/federation-upstream/%2f/up2", body: body)
        response.status_code.should eq 400
      end
    end

    it "refuses an in-process upstream the user can't access over the HTTP API" do
      with_http_server do |http, s|
        s.vhosts.create("secret")
        s.users.create("pm", "pass", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("pm", "/", /.*/, /.*/, /.*/)
        headers = HTTP::Headers{"Authorization" => "Basic #{Base64.strict_encode("pm:pass")}"}
        body = {"value" => {"uri" => "amqp:///secret"}}.to_json
        response = http.put("/api/parameters/federation-upstream/%2f/up", body: body, headers: headers)
        response.status_code.should eq 400
        body = {"value" => {"uri" => "amqp:///%2f"}}.to_json
        response = http.put("/api/parameters/federation-upstream/%2f/up", body: body, headers: headers)
        response.status_code.should eq 201
      end
    end

    it "removes a deleted upstream from sets" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        store = vhost.upstreams
        store.create_upstream("a", JSON.parse(%({"uri": "amqp:///"})))
        store.create_upstream("b", JSON.parse(%({"uri": "amqp:///"})))
        store.create_upstream_set("set", JSON.parse(%([{"upstream": "b"}, {"upstream": "a", "prefetch-count": 5}])))
        store.delete_upstream("a")
        store.get_set("set").map(&.name).should eq ["b"]
      end
    end

    it "overrides settings per set entry without changing the upstream" do
      with_amqp_server do |s|
        store = s.vhosts["/"].upstreams
        upstream = store.create_upstream("a", JSON.parse(%({"uri": "amqp:///", "prefetch-count": 10})))
        store.create_upstream_set("set", JSON.parse(%([{"upstream": "a", "prefetch-count": 5, "ack-mode": "no-ack"}])))
        entry = store.get_set("set").first
        entry.prefetch.should eq 5
        entry.ack_mode.should eq LavinMQ::Federation::AckMode::NoAck
        upstream.prefetch.should eq 10
        upstream.ack_mode.should eq LavinMQ::Federation::AckMode::OnConfirm
      end
    end
  end
end
