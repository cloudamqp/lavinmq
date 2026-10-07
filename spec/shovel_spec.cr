require "./spec_helper"
require "../src/lavinmq/shovel"
require "http/server"
require "wait_group"

module ShovelSpecHelpers
  # Every end-to-end scenario runs against both kinds of endpoint: this broker
  # in-process (a URI without host) and over AMQP (a URI with host).
  KINDS = {"in-process", "remote"}

  def self.uri(s : LavinMQ::Server, kind : String, vhost = "/") : URI
    path = URI.encode_path_segment(vhost)
    case kind
    when "in-process" then URI.parse("amqp:///#{path}")
    else                   URI.parse("#{s.amqp_server.url}/#{path}")
    end
  end

  def self.session(s, kind, vhost = "/", name = "spec") : LavinMQ::Endpoint::Session
    LavinMQ::Endpoint.session(uri(s, kind, vhost), s.vhosts["/"], name)
  end

  def self.source(s, kind, queue, vhost = "/", **opts) : LavinMQ::Shovel::AMQPSource
    LavinMQ::Shovel::AMQPSource.new("spec", [session(s, kind, vhost)], queue, **opts)
  end

  def self.destination(s, kind, queue, vhost = "/", **opts) : LavinMQ::Shovel::AMQPDestination
    LavinMQ::Shovel::AMQPDestination.new("spec", session(s, kind, vhost), queue, **opts)
  end

  # Creates a shovel the way the HTTP API and definitions do
  def self.create(vhost : LavinMQ::VHost, name : String, config) : LavinMQ::Shovel::Runner
    vhost.add_parameter(LavinMQ::Parameter.new("shovel", name, JSON.parse(config.to_json)))
    vhost.shovels[name]
  end

  def self.publish(vhost : LavinMQ::VHost, queue : String, body : String, headers = nil)
    props = AMQ::Protocol::Properties.new(headers: headers)
    vhost.publish(LavinMQ::Message.new("", queue, body, props))
  end

  def self.bodies(vhost : LavinMQ::VHost, queue : String) : Array(String)
    q = vhost.queue(queue)
    bodies = [] of String
    while q.basic_get(true) { |env| bodies << String.new(env.message.body) }
    end
    bodies
  end

  def self.delivery(tag : UInt64, body = "m") : LavinMQ::Endpoint::Delivery
    LavinMQ::Endpoint::Delivery.new(tag, "", "q", AMQ::Protocol::Properties.new, body.to_slice, false)
  end

  def self.http_server(&handler : HTTP::Server::Context -> Nil) : {HTTP::Server, Socket::IPAddress}
    server = HTTP::Server.new do |context|
      handler.call(context)
    end
    addr = server.bind_unused_port
    spawn server.listen
    {server, addr}
  end

  # Records every Outcome a Destination reports
  class RecordingListener
    include LavinMQ::Shovel::OutcomeListener
    getter outcomes = [] of {UInt64, LavinMQ::Shovel::Outcome}

    def report(delivery_tag : UInt64, outcome : LavinMQ::Shovel::Outcome)
      @outcomes << {delivery_tag, outcome}
    end
  end

  # A source that is never started and records settlements, for testing the
  # Runner's outcome handling in isolation
  class StoppedSource < LavinMQ::Shovel::Source
    getter delete_after = LavinMQ::Shovel::DeleteAfter::Never
    getter settlements = [] of {UInt64, Symbol}

    def start
    end

    def stop
    end

    def started? : Bool
      false
    end

    def each(&_blk : LavinMQ::Endpoint::Delivery -> Nil)
    end

    def ack(delivery_tag, batch = true)
      @settlements << {delivery_tag, :ack}
    end

    def reject(delivery_tag, requeue)
      @settlements << {delivery_tag, requeue ? :requeue : :reject}
    end
  end

  # A started source that records settlements
  class RecordingSource < StoppedSource
    def started? : Bool
      true
    end
  end

  # A destination whose start can be made to fail, counting calls
  class FakeDestination < LavinMQ::Shovel::Destination
    property start_error : Exception?
    getter starts = 0
    getter pushes = 0
    @started = false

    def initialize(@start_error : Exception? = nil)
    end

    def start
      @starts += 1
      if err = @start_error
        raise err
      end
      @started = true
    end

    def stop
      @started = false
    end

    def push(msg) : Nil
      @pushes += 1
    end

    def started? : Bool
      @started
    end
  end
end

describe LavinMQ::Endpoint do
  it "treats a URI without host as this broker" do
    LavinMQ::Endpoint.local?(URI.parse("amqp://")).should be_true
    LavinMQ::Endpoint.local?(URI.parse("amqp:///vhost")).should be_true
    LavinMQ::Endpoint.local?(URI.parse("amqp://localhost")).should be_false
    LavinMQ::Endpoint.local?(URI.parse("amqp://guest:guest@localhost/vhost")).should be_false
    LavinMQ::Endpoint.local?(URI.parse("http:///path")).should be_false
  end

  it "reads the vhost from the URI path" do
    LavinMQ::Endpoint.vhost_name(URI.parse("amqp://")).should eq "/"
    LavinMQ::Endpoint.vhost_name(URI.parse("amqp:///")).should eq "/"
    LavinMQ::Endpoint.vhost_name(URI.parse("amqp:///%2f")).should eq "/"
    LavinMQ::Endpoint.vhost_name(URI.parse("amqp:///my%20vhost")).should eq "my vhost"
  end

  it "displays URIs without credentials" do
    LavinMQ::Endpoint.display_uri(URI.parse("amqp://user:secret@host/v")).should eq "amqp://host/v"
    LavinMQ::Endpoint.display_uri(URI.parse("amqp:///v")).should eq "amqp:///v"
  end

  describe LavinMQ::Endpoint::LocalSession do
    it "doesn't open an AMQP connection" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_q", true, false)
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        session.publish("", "ls_q", AMQ::Protocol::Properties.new, "hello".to_slice)
        vhost.connections_size.should eq 0
        ShovelSpecHelpers.bodies(vhost, "ls_q").should eq ["hello"]
        session.close
      end
    end

    it "confirms a publish once it is durable" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_confirm", true, false)
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        confirmed = Channel(Bool).new(1)
        session.publish("", "ls_confirm", AMQ::Protocol::Properties.new, "m".to_slice) do |ok|
          confirmed.send ok
        end
        confirmed.receive.should be_true
        session.close
      end
    end

    it "nacks a publish refused by a reject-publish queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        args = AMQ::Protocol::Table.new({"x-max-length" => 1_i64, "x-overflow" => "reject-publish"})
        vhost.declare_queue("ls_full", true, false, args)
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        results = Channel(Bool).new(2)
        2.times do
          session.publish("", "ls_full", AMQ::Protocol::Properties.new, "m".to_slice) { |ok| results.send ok }
        end
        [results.receive, results.receive].count(true).should eq 1
        session.close
      end
    end

    it "nacks pending confirms when closed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_pending", true, false)
        session = ShovelSpecHelpers.session(s, "in-process").as(LavinMQ::Endpoint::LocalSession)
        session.open
        results = [] of Bool
        # Hold the confirm: it's only delivered by the persister later
        session.publish("", "ls_pending", AMQ::Protocol::Properties.new, "m".to_slice) { |ok| results << ok }
        session.close
        should_eventually(eq 1) { results.size }
      end
    end

    it "refuses to publish to a missing or internal exchange" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_exchange("ls_internal", "direct", true, false, internal: true)
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        expect_raises(LavinMQ::Endpoint::NotFound) do
          session.publish("ls_missing", "", AMQ::Protocol::Properties.new, "m".to_slice)
        end
        expect_raises(LavinMQ::Endpoint::Refused) do
          session.publish("ls_internal", "", AMQ::Protocol::Properties.new, "m".to_slice)
        end
        session.close
      end
    end

    it "registers its consumer on the queue and requeues unacked deliveries on close" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_consume", true, false)
        3.times { |i| ShovelSpecHelpers.publish(vhost, "ls_consume", "m#{i}") }
        q = vhost.queue("ls_consume")
        session = ShovelSpecHelpers.session(s, "in-process", name: "Spec session")
        session.open
        session.prefetch = 10_u16
        tags = Channel(UInt64).new(3)
        spawn do
          session.consume("ls_consume", "spec", false, false, AMQ::Protocol::Table.new) do |d|
            tags.send d.tag
          end
        rescue LavinMQ::Endpoint::ClosedError
        end
        Array.new(3) { tags.receive }.should eq [1_u64, 2_u64, 3_u64]
        q.consumer_count.should eq 1
        q.unacked_count.should eq 3
        consumer = JSON.parse(q.to_json)["consumer_details"][0]
        consumer["consumer_tag"].should eq "spec"
        consumer["channel_details"]["connection_name"].should eq "Spec session"

        session.ack(1_u64)
        session.reject(2_u64, requeue: false)
        should_eventually(eq 1) { q.unacked_count }
        session.close
        should_eventually(eq 0) { q.consumer_count }
        q.unacked_count.should eq 0
        q.message_count.should eq 1 # m2 was dropped, m3 requeued
      end
    end

    it "acks cumulatively with multiple" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_multi", true, false)
        3.times { |i| ShovelSpecHelpers.publish(vhost, "ls_multi", "m#{i}") }
        q = vhost.queue("ls_multi")
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        session.prefetch = 10_u16
        delivered = WaitGroup.new(3)
        spawn do
          session.consume("ls_multi", "spec", false, false, AMQ::Protocol::Table.new) { delivered.done }
        rescue LavinMQ::Endpoint::ClosedError
        end
        delivered.wait
        session.ack(2_u64, multiple: true)
        should_eventually(eq 1) { q.unacked_count }
        session.close
        q.message_count.should eq 1
      end
    end

    it "respects the prefetch limit" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_prefetch", true, false)
        5.times { |i| ShovelSpecHelpers.publish(vhost, "ls_prefetch", "m#{i}") }
        q = vhost.queue("ls_prefetch")
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        session.prefetch = 2_u16
        count = Atomic(Int32).new(0)
        spawn do
          session.consume("ls_prefetch", "spec", false, false, AMQ::Protocol::Table.new) { count.add(1) }
        rescue LavinMQ::Endpoint::ClosedError
        end
        should_eventually(eq 2) { count.get }
        sleep 20.milliseconds
        count.get.should eq 2
        session.ack(1_u64)
        should_eventually(eq 3) { count.get }
        session.close
      end
    end

    it "ends consuming when the queue is deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_deleted", true, false)
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        done = Channel(Nil).new
        spawn do
          session.consume("ls_deleted", "spec", false, false, AMQ::Protocol::Table.new) { }
          done.close
        end
        wait_for { vhost.queue("ls_deleted").consumer_count == 1 }
        vhost.delete_queue("ls_deleted")
        select
        when done.receive?
        when timeout(5.seconds)
          fail "consume didn't return when its queue was deleted"
        end
        session.close
      end
    end

    it "refuses exclusive queues and queues in exclusive use" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.queue("ls_exclusive", exclusive: true)
          q = ch.queue("ls_in_use")
          q.subscribe(exclusive: true) { }
          session = ShovelSpecHelpers.session(s, "in-process")
          session.open
          expect_raises(LavinMQ::Endpoint::Refused) do
            session.consume("ls_exclusive", "spec", false, false, AMQ::Protocol::Table.new) { }
          end
          expect_raises(LavinMQ::Endpoint::Refused) do
            session.consume("ls_in_use", "spec", false, false, AMQ::Protocol::Table.new) { }
          end
          vhost.queue("ls_in_use").consumer_count.should eq 1
          session.close
        end
      end
    end

    it "consumes a stream from an offset, following its tail" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_stream", true, false, AMQ::Protocol::Table.new({"x-queue-type" => "stream"}))
        3.times { |i| ShovelSpecHelpers.publish(vhost, "ls_stream", "m#{i}") }
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        session.prefetch = 10_u16
        {"first" => ["m0", "m1", "m2", "new-first"], "next" => ["new-next"]}.each do |offset, expected|
          bodies = Channel(String).new(10)
          args = AMQ::Protocol::Table.new({"x-stream-offset" => offset})
          spawn do
            session.consume("ls_stream", "spec-#{offset}", false, false, args) do |d|
              bodies.send String.new(d.body)
              session.ack(d.tag)
            end
          rescue LavinMQ::Endpoint::ClosedError
          end
          wait_for { vhost.queue("ls_stream").consumer_count == 1 }
          ShovelSpecHelpers.publish(vhost, "ls_stream", "new-#{offset}")
          expected.each { |body| bodies.receive.should eq body }
          session.cancel("spec-#{offset}")
          wait_for { vhost.queue("ls_stream").consumer_count == 0 }
          bodies.try_receive?.should be_nil
        end
        session.close
      end
    end

    it "refuses no-ack consumers on a stream" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_stream_na", true, false, AMQ::Protocol::Table.new({"x-queue-type" => "stream"}))
        session = ShovelSpecHelpers.session(s, "in-process")
        session.open
        session.prefetch = 10_u16
        expect_raises(LavinMQ::Endpoint::Refused, /acknowledge/) do
          session.consume("ls_stream_na", "spec", true, false, AMQ::Protocol::Table.new) { }
        end
        vhost.queue("ls_stream_na").consumer_count.should eq 0
        session.close
      end
    end

    it "doesn't register a delivery once closed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ls_closed", true, false)
        q = vhost.queue("ls_closed")
        session = ShovelSpecHelpers.session(s, "in-process").as(LavinMQ::Endpoint::LocalSession)
        session.open
        consumer = LavinMQ::Endpoint::LocalConsumer.new(session, q, "spec", false, false, 10_u16)
        sp = LavinMQ::SegmentPosition.new(1_u32, 4_u32, 10_u32)
        session.close
        # A delivery fetched while the session closed must not be tracked
        # after close requeued everything it had
        session.next_delivery_tag(consumer, sp).should be_nil
        session.@unacked.should be_empty
      end
    end

    it "closes, and says so, when its vhost is deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("ls_vhost")
        vhost.declare_queue("q", true, false)
        session = LavinMQ::Endpoint.session(URI.parse("amqp:///ls_vhost"), s.vhosts["/"], "spec")
        reasons = Channel(String).new(1)
        session.on_close { |reason| reasons.send reason }
        session.open
        s.vhosts.delete("ls_vhost")
        reasons.receive.should contain "closed"
        session.closed?.should be_true
      end
    end
  end

  describe LavinMQ::Endpoint::RemoteSession do
    it "turns a failed passive declare into NotFound and stays usable" do
      with_amqp_server do |s|
        session = ShovelSpecHelpers.session(s, "remote")
        session.open
        expect_raises(LavinMQ::Endpoint::NotFound) do
          session.declare_queue("rs_missing", passive: true)
        end
        session.declare_queue("rs_missing", passive: false)[0].should eq "rs_missing"
        session.close
      end
    end
  end
end

describe LavinMQ::Shovel do
  ShovelSpecHelpers::KINDS.each do |kind|
    describe "with #{kind} endpoints" do
      it "moves messages between queues" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("mv_q1", true, false)
          3.times { |i| ShovelSpecHelpers.publish(vhost, "mv_q1", "m#{i}") }
          source = ShovelSpecHelpers.source(s, kind, "mv_q1")
          dest = ShovelSpecHelpers.destination(s, kind, "mv_q2")
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "mv", vhost)
          spawn shovel.run
          should_eventually(eq 3) { vhost.queue?("mv_q2").try(&.message_count) }
          vhost.queue("mv_q1").message_count.should eq 0
          should_eventually(eq 0) { vhost.queue("mv_q1").unacked_count }
          ShovelSpecHelpers.bodies(vhost, "mv_q2").should eq ["m0", "m1", "m2"]
          shovel.terminate
        end
      end

      it "keeps message properties" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("prop_q1", true, false)
          vhost.declare_queue("prop_q2", true, false)
          props = AMQ::Protocol::Properties.new(content_type: "text/plain", message_id: "id-1",
            headers: AMQ::Protocol::Table.new({"h" => "v"}))
          vhost.publish(LavinMQ::Message.new("", "prop_q1", "body", props))
          shovel = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers.source(s, kind, "prop_q1"),
            ShovelSpecHelpers.destination(s, kind, "prop_q2"), "prop", vhost)
          spawn shovel.run
          should_eventually(eq 1) { vhost.queue("prop_q2").message_count }
          vhost.queue("prop_q2").basic_get(true) do |env|
            env.message.properties.content_type.should eq "text/plain"
            env.message.properties.message_id.should eq "id-1"
            env.message.properties.headers.not_nil!["h"].should eq "v"
          end.should be_true
          shovel.terminate
        end
      end

      it "moves messages from an exchange through a bound queue" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("ex_q2", true, false)
          source = ShovelSpecHelpers.source(s, kind, nil, exchange: "amq.topic", exchange_key: "a.#")
          dest = ShovelSpecHelpers.destination(s, kind, "ex_q2")
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "ex", vhost)
          spawn shovel.run
          wait_for { shovel.running? }
          vhost.publish(LavinMQ::Message.new("amq.topic", "a.b", "match"))
          vhost.publish(LavinMQ::Message.new("amq.topic", "b.a", "no match"))
          should_eventually(eq ["match"]) { ShovelSpecHelpers.bodies(vhost, "ex_q2") }
          shovel.terminate
        end
      end

      it "publishes to a destination exchange, keeping the routing key" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("dx_q1", true, false)
          vhost.declare_queue("dx_q2", true, false)
          vhost.bind_queue("dx_q2", "amq.direct", "dx_q1")
          ShovelSpecHelpers.publish(vhost, "dx_q1", "m")
          dest = ShovelSpecHelpers.destination(s, kind, nil, exchange: "amq.direct")
          shovel = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers.source(s, kind, "dx_q1"), dest, "dx", vhost)
          spawn shovel.run
          should_eventually(eq 1) { vhost.queue("dx_q2").message_count }
          shovel.terminate
        end
      end

      it "moves large messages" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("lg_q1", true, false)
          body = "x" * 1_000_000
          ShovelSpecHelpers.publish(vhost, "lg_q1", body)
          shovel = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers.source(s, kind, "lg_q1"),
            ShovelSpecHelpers.destination(s, kind, "lg_q2"), "lg", vhost)
          spawn shovel.run
          should_eventually(eq 1) { vhost.queue?("lg_q2").try(&.message_count) }
          ShovelSpecHelpers.bodies(vhost, "lg_q2").should eq [body]
          shovel.terminate
        end
      end

      {% for mode in %w[OnConfirm OnPublish NoAck] %}
        it "moves messages with ack mode {{ mode.id }}" do
          with_amqp_server do |s|
            vhost = s.vhosts["/"]
            vhost.declare_queue("am_q1", true, false)
            50.times { |i| ShovelSpecHelpers.publish(vhost, "am_q1", "m#{i}") }
            ack_mode = LavinMQ::Shovel::AckMode::{{ mode.id }}
            source = ShovelSpecHelpers.source(s, kind, "am_q1", ack_mode: ack_mode, prefetch: 7_u16)
            dest = ShovelSpecHelpers.destination(s, kind, "am_q2", ack_mode: ack_mode)
            shovel = LavinMQ::Shovel::Runner.new(source, dest, "am", vhost)
            spawn shovel.run
            should_eventually(eq 50) { vhost.queue?("am_q2").try(&.message_count) }
            q1 = vhost.queue("am_q1")
            should_eventually(eq 0) { q1.message_count + q1.unacked_count }
            shovel.terminate
          end
        end
      {% end %}

      it "stops and deletes itself once queue-length messages are moved" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("ql_q1", true, false)
          vhost.declare_queue("ql_q2", true, false)
          3.times { |i| ShovelSpecHelpers.publish(vhost, "ql_q1", "m#{i}") }
          shovel = ShovelSpecHelpers.create(vhost, "ql", {
            "src-uri"          => ShovelSpecHelpers.uri(s, kind).to_s,
            "src-queue"        => "ql_q1",
            "dest-uri"         => ShovelSpecHelpers.uri(s, kind).to_s,
            "dest-queue"       => "ql_q2",
            "src-delete-after" => "queue-length",
          })
          should_eventually(be_true) { shovel.terminated? }
          vhost.shovels.empty?.should be_true
          vhost.parameters.empty?.should be_true
          vhost.queue("ql_q2").message_count.should eq 3
          should_eventually(eq 0) { vhost.queue("ql_q1").unacked_count }
          vhost.queue("ql_q1").message_count.should eq 0
        end
      end

      it "doesn't lose source messages when the destination rejects publishes" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("rp_q1", true, false)
          args = AMQ::Protocol::Table.new({"x-max-length" => 2_i64, "x-overflow" => "reject-publish"})
          vhost.declare_queue("rp_q2", true, false, args)
          5.times { |i| ShovelSpecHelpers.publish(vhost, "rp_q1", "m#{i}") }
          shovel = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers.source(s, kind, "rp_q1"),
            ShovelSpecHelpers.destination(s, kind, "rp_q2"), "rp", vhost)
          spawn shovel.run
          should_eventually(eq 2) { vhost.queue("rp_q2").message_count }
          should_eventually(be > 0) { shovel.details_tuple[:retried] }
          q1 = vhost.queue("rp_q1")
          # The two accepted are acked once their confirms arrive (on fsync,
          # slow on macOS); terminating before that would requeue them too.
          should_eventually(eq 3) { q1.message_count + q1.unacked_count }
          shovel.terminate
          should_eventually(eq 3) { q1.message_count }
          q1.unacked_count.should eq 0
        end
      end

      it "moves a stream from the first offset" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("st_q1", true, false, AMQ::Protocol::Table.new({"x-queue-type" => "stream"}))
          3.times { |i| ShovelSpecHelpers.publish(vhost, "st_q1", "m#{i}") }
          shovel = ShovelSpecHelpers.create(vhost, "st", {
            "src-uri"           => ShovelSpecHelpers.uri(s, kind).to_s,
            "src-queue"         => "st_q1",
            "src-consumer-args" => {"x-stream-offset" => "first"},
            "dest-uri"          => ShovelSpecHelpers.uri(s, kind).to_s,
            "dest-queue"        => "st_q2",
          })
          should_eventually(eq 3) { vhost.queue?("st_q2").try(&.message_count) }
          ShovelSpecHelpers.publish(vhost, "st_q1", "m3")
          should_eventually(eq 4) { vhost.queue("st_q2").message_count }
          shovel.terminate
        end
      end

      it "moves messages between vhosts" do
        with_amqp_server do |s|
          src_vhost = s.vhosts.create("shovel-a")
          dst_vhost = s.vhosts.create("shovel-b")
          src_vhost.declare_queue("q", true, false)
          dst_vhost.declare_queue("q", true, false)
          2.times { |i| ShovelSpecHelpers.publish(src_vhost, "q", "m#{i}") }
          shovel = ShovelSpecHelpers.create(src_vhost, "cross", {
            "src-uri"    => ShovelSpecHelpers.uri(s, kind, "shovel-a").to_s,
            "src-queue"  => "q",
            "dest-uri"   => ShovelSpecHelpers.uri(s, kind, "shovel-b").to_s,
            "dest-queue" => "q",
          })
          should_eventually(eq 2) { dst_vhost.queue("q").message_count }
          src_vhost.queue("q").message_count.should eq 0
          shovel.terminate
        end
      end

      it "pauses and resumes" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("pz_q1", true, false)
          shovel = ShovelSpecHelpers.create(vhost, "pz", {
            "src-uri"    => ShovelSpecHelpers.uri(s, kind).to_s,
            "src-queue"  => "pz_q1",
            "dest-uri"   => ShovelSpecHelpers.uri(s, kind).to_s,
            "dest-queue" => "pz_q2",
          })
          wait_for { shovel.running? }
          shovel.pause
          shovel.paused?.should be_true
          q1 = vhost.queue("pz_q1")
          should_eventually(eq 0) { q1.consumer_count }
          ShovelSpecHelpers.publish(vhost, "pz_q1", "while paused")
          sleep 20.milliseconds
          q1.message_count.should eq 1
          shovel.resume
          should_eventually(eq 1) { vhost.queue("pz_q2").message_count }
          shovel.terminate
        end
      end

      it "reconnects and continues after a broker restart" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("rc_q1", true, false)
          vhost.declare_queue("rc_q2", true, false)
          ShovelSpecHelpers.create(vhost, "rc", {
            "src-uri"         => ShovelSpecHelpers.uri(s, kind).to_s,
            "src-queue"       => "rc_q1",
            "dest-uri"        => ShovelSpecHelpers.uri(s, kind).to_s,
            "dest-queue"      => "rc_q2",
            "reconnect-delay" => 1,
          })
          wait_for { vhost.shovels["rc"].running? }
          restart_server(s)
          vhost = s.vhosts["/"]
          should_eventually(be_true) { vhost.shovels["rc"]?.try(&.running?) }
          ShovelSpecHelpers.publish(vhost, "rc_q1", "after restart")
          should_eventually(eq 1) { vhost.queue("rc_q2").message_count }
        end
      end

      it "stops when the source queue is deleted" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("del_q1", true, false)
          shovel = ShovelSpecHelpers.create(vhost, "del", {
            "src-uri"    => ShovelSpecHelpers.uri(s, kind).to_s,
            "src-queue"  => "del_q1",
            "dest-uri"   => ShovelSpecHelpers.uri(s, kind).to_s,
            "dest-queue" => "del_q2",
          })
          wait_for { shovel.running? }
          vhost.delete_queue("del_q1")
          should_eventually(be_true) { shovel.terminated? }
        end
      end

      it "counts moved messages" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("cnt_q1", true, false)
          4.times { |i| ShovelSpecHelpers.publish(vhost, "cnt_q1", "m#{i}") }
          shovel = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers.source(s, kind, "cnt_q1"),
            ShovelSpecHelpers.destination(s, kind, "cnt_q2"), "cnt", vhost)
          spawn shovel.run
          should_eventually(eq 4) { shovel.details_tuple[:confirmed] }
          shovel.details_tuple[:message_count].should eq 4
          shovel.terminate
        end
      end
    end
  end

  describe "in-process" do
    it "works whatever port the AMQP listener uses (no loopback connection)" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("np_q1", true, false)
        ShovelSpecHelpers.publish(vhost, "np_q1", "m")
        # The server under test listens on a random port; a shovel looping back
        # over AMQP to the default port can't reach it.
        shovel = ShovelSpecHelpers.create(vhost, "np", {
          "src-uri"    => "amqp://",
          "src-queue"  => "np_q1",
          "dest-uri"   => "amqp://",
          "dest-queue" => "np_q2",
        })
        should_eventually(eq 1) { vhost.queue?("np_q2").try(&.message_count) }
        vhost.connections_size.should eq 0
        shovel.state.running?.should be_true
        shovel.terminate
      end
    end

    it "shows as a consumer of the source queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("cs_q1", true, false)
        shovel = ShovelSpecHelpers.create(vhost, "cs", {
          "src-uri"    => "amqp://",
          "src-queue"  => "cs_q1",
          "dest-uri"   => "amqp://",
          "dest-queue" => "cs_q2",
        })
        q = vhost.queue("cs_q1")
        should_eventually(eq 1) { q.consumer_count }
        consumer = JSON.parse(q.to_json)["consumer_details"][0]
        consumer["consumer_tag"].should eq "Shovel"
        consumer["channel_details"]["connection_name"].should eq "Shovel cs source"
        shovel.terminate
        should_eventually(eq 0) { q.consumer_count }
      end
    end

    it "returns messages in flight to the source when deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("if_q1", true, false)
        3.times { |i| ShovelSpecHelpers.publish(vhost, "if_q1", "m#{i}") }
        holding = Channel(Nil).new
        release = Channel(Nil).new
        server, addr = ShovelSpecHelpers.http_server do |context|
          context.request.body.try &.skip_to_end
          holding.send nil
          release.receive?
          context.response.status_code = 200
        end
        ShovelSpecHelpers.create(vhost, "if", {
          "src-uri"   => "amqp://",
          "src-queue" => "if_q1",
          "dest-uri"  => "http://#{addr}/",
        })
        holding.receive
        q = vhost.queue("if_q1")
        q.unacked_count.should be > 0
        spawn { vhost.delete_parameter("shovel", "if") }
        sleep 10.milliseconds
        release.close
        should_eventually(eq 0) { q.unacked_count }
        q.consumer_count.should eq 0
        (q.message_count).should be >= 2
      ensure
        server.try &.close
      end
    end

    it "keeps a paused shovel paused across broker restarts" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("kp_q1", true, false)
        shovel = ShovelSpecHelpers.create(vhost, "kp", {
          "src-uri"    => "amqp://",
          "src-queue"  => "kp_q1",
          "dest-uri"   => "amqp://",
          "dest-queue" => "kp_q2",
        })
        wait_for { shovel.running? }
        shovel.pause
        restart_server(s)
        should_eventually(be_true) { s.vhosts["/"].shovels["kp"]?.try(&.paused?) }
        s.vhosts["/"].shovels["kp"].resume
        should_eventually(be_true) { s.vhosts["/"].shovels["kp"].running? }
      end
    end

    it "retries with backoff while the destination vhost is missing" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("mv_src", true, false)
        shovel = ShovelSpecHelpers.create(vhost, "missing", {
          "src-uri"         => "amqp://",
          "src-queue"       => "mv_src",
          "dest-uri"        => "amqp:///not-yet",
          "dest-queue"      => "q",
          "reconnect-delay" => 1,
        })
        should_eventually(be_true) { shovel.state.error? }
        shovel.details_tuple[:error].to_s.should contain "not-yet"
        s.vhosts.create("not-yet")
        should_eventually(be_true, 5.seconds) { shovel.running? }
        shovel.terminate
      end
    end
  end

  describe "AMQPSource" do
    it "batches acks to a remote broker when each ack is reported during its delivery" do
      # Like an on-publish or HTTP destination: nothing is ever in flight
      # between deliveries, yet more are coming, so batching must hold.
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ba_q", true, false)
        q = vhost.queue("ba_q")
        3.times { |i| ShovelSpecHelpers.publish(vhost, "ba_q", "m#{i}") }
        source = ShovelSpecHelpers.source(s, "remote", "ba_q", prefetch: 10_u16, batch_ack_timeout: 1.hour)
        source.start
        acked = Channel(UInt64).new(5)
        spawn do
          source.each do |m|
            source.ack(m.tag)
            acked.send m.tag
          end
        rescue
        end
        3.times { acked.receive }
        sleep 50.milliseconds
        q.unacked_count.should eq 3 # waiting for a batch of 5
        2.times { |i| ShovelSpecHelpers.publish(vhost, "ba_q", "n#{i}") }
        should_eventually(eq(0), 1.second) { q.unacked_count }
        q.message_count.should eq 0
        source.stop
      end
    end

    it "flushes a partial batch at the timeout while deliveries are in flight" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("bt_q", true, false)
        q = vhost.queue("bt_q")
        3.times { |i| ShovelSpecHelpers.publish(vhost, "bt_q", "m#{i}") }
        source = ShovelSpecHelpers.source(s, "remote", "bt_q", prefetch: 10_u16,
          batch_ack_timeout: 300.milliseconds)
        source.start
        tags = Channel(UInt64).new(3)
        spawn { source.each { |m| tags.send m.tag } rescue nil }
        delivered = Array.new(3) { tags.receive }
        source.ack(delivered[0]) # the other two stay in flight
        q.unacked_count.should eq 3
        should_eventually(eq(2), 2.seconds) { q.unacked_count }
        source.stop
      end
    end

    it "flushes an ack that a requeue unblocked, at the timeout" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("br_q", true, false)
        q = vhost.queue("br_q")
        3.times { |i| ShovelSpecHelpers.publish(vhost, "br_q", "m#{i}") }
        source = ShovelSpecHelpers.source(s, "remote", "br_q", prefetch: 10_u16,
          batch_ack_timeout: 100.milliseconds)
        source.start
        tags = Channel(UInt64).new(4)
        spawn { source.each { |m| tags.send m.tag } rescue nil }
        delivered = Array.new(3) { tags.receive }
        source.ack(delivered[1]) # out of order: waits above the frontier
        source.reject(delivered[0], requeue: true)
        tags.receive # the requeued message, redelivered and in flight
        # Tag 2 is now pending behind the frontier while 3 and 4 are in flight
        should_eventually(eq(2), 2.seconds) { q.unacked_count }
        source.stop
      end
    end

    it "acks in-process deliveries right away" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ia_q", true, false)
        q = vhost.queue("ia_q")
        ShovelSpecHelpers.publish(vhost, "ia_q", "m")
        source = ShovelSpecHelpers.source(s, "in-process", "ia_q", prefetch: 10_u16, batch_ack_timeout: 1.hour)
        source.start
        tags = Channel(UInt64).new(1)
        spawn { source.each { |m| tags.send m.tag } rescue nil }
        source.ack(tags.receive)
        q.unacked_count.should eq 0
        source.stop
      end
    end

    ShovelSpecHelpers::KINDS.each do |kind|
      it "acks cumulatively only up to the highest acked tag (#{kind})" do
        with_amqp_server do |s|
          vhost = s.vhosts["/"]
          vhost.declare_queue("cf_q", true, false)
          q = vhost.queue("cf_q")
          3.times { |i| ShovelSpecHelpers.publish(vhost, "cf_q", "m#{i}") }
          source = ShovelSpecHelpers.source(s, kind, "cf_q", prefetch: 4_u16, batch_ack_timeout: 1.hour)
          source.start
          delivered = Channel(UInt64).new(8)
          spawn { source.each { |m| delivered.send m.tag } rescue nil }
          tags = Array.new(3) { delivered.receive }
          # Confirms out of order: 3 and 2 are requeued before 1 is acked. The
          # cumulative ack must name tag 1, never the already rejected 3.
          source.reject(tags[2], requeue: true)
          source.reject(tags[1], requeue: true)
          source.ack(tags[0])
          2.times do
            select
            when tag = delivered.receive
              source.ack(tag, batch: false)
            when timeout(2.seconds)
              fail "requeued messages were not redelivered"
            end
          end
          should_eventually(eq 0) { q.unacked_count }
          q.message_count.should eq 0
          q.consumer_count.should eq 1
          source.stop
        end
      end
    end

    it "raises when its remote connection is closed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("cl_q", true, false)
        ShovelSpecHelpers.publish(vhost, "cl_q", "m")
        source = ShovelSpecHelpers.source(s, "remote", "cl_q")
        source.start
        expect_raises(Exception) do
          source.each { vhost.each_connection &.close("spec") }
        end
        source.started?.should be_false
      end
    end
  end

  describe "HTTP destination" do
    it "posts messages" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("hp_q", true, false)
        props = AMQ::Protocol::Properties.new(content_type: "text/plain", message_id: "id-1",
          headers: AMQ::Protocol::Table.new({"a" => "b"}))
        vhost.publish(LavinMQ::Message.new("", "hp_q", "hello", props))
        requests = Channel(HTTP::Request).new(1)
        bodies = Channel(String).new(1)
        server, addr = ShovelSpecHelpers.http_server do |context|
          bodies.send context.request.body.try(&.gets_to_end).to_s
          requests.send context.request
        end
        shovel = ShovelSpecHelpers.create(vhost, "hp", {
          "src-uri"   => "amqp://",
          "src-queue" => "hp_q",
          "dest-uri"  => "http://user:pass@#{addr}/hook",
        })
        bodies.receive.should eq "hello"
        req = requests.receive
        req.path.should eq "/hook"
        req.headers["Content-Type"].should eq "text/plain"
        req.headers["X-Message-Id"].should eq "id-1"
        req.headers["X-a"].should eq "b"
        req.headers["X-Shovel"].should eq "hp"
        req.headers["Authorization"].should eq "Basic #{Base64.strict_encode("user:pass")}"
        should_eventually(eq 0) { vhost.queue("hp_q").message_count + vhost.queue("hp_q").unacked_count }
        shovel.terminate
      ensure
        server.try &.close
      end
    end

    it "takes the path from the uri_path header when the URI has none" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("up_q", true, false)
        ShovelSpecHelpers.publish(vhost, "up_q", "m", AMQ::Protocol::Table.new({"uri_path" => "/from/header"}))
        paths = Channel(String).new(1)
        server, addr = ShovelSpecHelpers.http_server do |context|
          context.request.body.try &.skip_to_end
          paths.send context.request.path
        end
        shovel = ShovelSpecHelpers.create(vhost, "up", {
          "src-uri"   => "amqp://",
          "src-queue" => "up_q",
          "dest-uri"  => "http://#{addr}",
        })
        paths.receive.should eq "/from/header"
        shovel.terminate
      ensure
        server.try &.close
      end
    end

    it "requeues on a server error and dead-letters a rejected message" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("hd_dlq", true, false)
        vhost.declare_queue("hd_q", true, false, AMQ::Protocol::Table.new({
          "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "hd_dlq",
        }))
        ShovelSpecHelpers.publish(vhost, "hd_q", "bad")
        ShovelSpecHelpers.publish(vhost, "hd_q", "busy")
        busy_seen = Atomic(Int32).new(0)
        server, addr = ShovelSpecHelpers.http_server do |context|
          case context.request.body.try(&.gets_to_end)
          when "bad" then context.response.status_code = 400
          else
            context.response.status_code = busy_seen.add(1) < 2 ? 503 : 200
          end
        end
        shovel = ShovelSpecHelpers.create(vhost, "hd", {
          "src-uri"   => "amqp://",
          "src-queue" => "hd_q",
          "dest-uri"  => "http://#{addr}/",
        })
        should_eventually(eq 1) { vhost.queue("hd_dlq").message_count }
        should_eventually(eq 0) { vhost.queue("hd_q").message_count + vhost.queue("hd_q").unacked_count }
        busy_seen.get.should be >= 3
        shovel.details_tuple[:rejected].should eq 1
        shovel.details_tuple[:retried].should be >= 2
        shovel.terminate
      ensure
        server.try &.close
      end
    end

    it "aborts after repeated aborts and can be resumed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("ha_q", true, false)
        ShovelSpecHelpers.publish(vhost, "ha_q", "m")
        status = Atomic(Int32).new(404)
        server, addr = ShovelSpecHelpers.http_server do |context|
          context.request.body.try &.skip_to_end
          context.response.status_code = status.get
        end
        shovel = ShovelSpecHelpers.create(vhost, "ha", {
          "src-uri"   => "amqp://",
          "src-queue" => "ha_q",
          "dest-uri"  => "http://#{addr}/",
        })
        should_eventually(be_true, 10.seconds) { shovel.aborted? }
        vhost.queue("ha_q").message_count.should eq 1 # the message is kept
        status.set(200)
        shovel.resume
        should_eventually(eq 0) { vhost.queue("ha_q").message_count + vhost.queue("ha_q").unacked_count }
        shovel.terminate
      ensure
        server.try &.close
      end
    end

    it "classifies statuses" do
      dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://localhost/"))
      {
        200 => LavinMQ::Shovel::Outcome::Confirmed,
        204 => LavinMQ::Shovel::Outcome::Confirmed,
        503 => LavinMQ::Shovel::Outcome::Retry,
        429 => LavinMQ::Shovel::Outcome::Retry,
        408 => LavinMQ::Shovel::Outcome::Retry,
        400 => LavinMQ::Shovel::Outcome::Reject,
        413 => LavinMQ::Shovel::Outcome::Reject,
        422 => LavinMQ::Shovel::Outcome::Reject,
        401 => LavinMQ::Shovel::Outcome::Abort,
        404 => LavinMQ::Shovel::Outcome::Abort,
      }.each do |code, outcome|
        dest.classify(HTTP::Client::Response.new(code)).should eq outcome
      end
    end

    it "reports Retry when the endpoint is unreachable" do
      dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://127.0.0.1:1/"), timeout: 1.second)
      listener = ShovelSpecHelpers::RecordingListener.new
      dest.listener = listener
      dest.start
      dest.push(ShovelSpecHelpers.delivery(1_u64))
      listener.outcomes.should eq [{1_u64, LavinMQ::Shovel::Outcome::Retry}]
      dest.stop
    end

    it "parses dest-timeout" do
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({}))).should eq 30.seconds
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": 5}))).should eq 5.seconds
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": 0.5}))).should eq 0.5.seconds
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": -1}))).should eq 30.seconds
    end
  end

  describe LavinMQ::Shovel::Runner do
    it "maps outcomes to source actions" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::RecordingSource.new
        runner = LavinMQ::Shovel::Runner.new(source, ShovelSpecHelpers::FakeDestination.new, "spec", s.vhosts["/"])
        runner.report(1_u64, LavinMQ::Shovel::Outcome::Confirmed)
        runner.report(2_u64, LavinMQ::Shovel::Outcome::Retry)
        runner.report(3_u64, LavinMQ::Shovel::Outcome::Reject)
        runner.report(4_u64, LavinMQ::Shovel::Outcome::Abort)
        source.settlements.should eq [{1_u64, :ack}, {2_u64, :requeue}, {3_u64, :reject}, {4_u64, :requeue}]
        d = runner.details_tuple
        {d[:confirmed], d[:retried], d[:rejected], d[:aborted]}.should eq({1, 1, 1, 1})
      end
    end

    it "ignores outcomes once the source is stopped" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::StoppedSource.new
        runner = LavinMQ::Shovel::Runner.new(source, ShovelSpecHelpers::FakeDestination.new, "spec", s.vhosts["/"])
        runner.report(1_u64, LavinMQ::Shovel::Outcome::Retry)
        source.settlements.should be_empty
        runner.details_tuple[:retried].should eq 0
      end
    end

    it "backs off 0.5s, doubling, up to 30s" do
      LavinMQ::Shovel::Runner.delivery_backoff(0).should eq 0.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(1).should eq 0.5.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(2).should eq 1.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(4).should eq 4.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(20).should eq 30.seconds
    end

    it "counts a burst of Retry outcomes as one failing round" do
      with_amqp_server do |s|
        runner = LavinMQ::Shovel::Runner.new(ShovelSpecHelpers::RecordingSource.new,
          ShovelSpecHelpers::FakeDestination.new, "spec", s.vhosts["/"])
        5.times { |i| runner.report(i.to_u64, LavinMQ::Shovel::Outcome::Retry) }
        runner.details_tuple[:consecutive_failures].should eq 1
        runner.pending_backoff.should be > 0.seconds
        runner.report(9_u64, LavinMQ::Shovel::Outcome::Confirmed)
        runner.pending_backoff.should eq 0.seconds
      end
    end
  end

  describe LavinMQ::Shovel::MultiDestination do
    it "draws one destination per start" do
      a = ShovelSpecHelpers::FakeDestination.new
      b = ShovelSpecHelpers::FakeDestination.new
      multi = LavinMQ::Shovel::MultiDestination.new([a, b] of LavinMQ::Shovel::Destination)
      20.times do
        multi.start
        multi.push(ShovelSpecHelpers.delivery(1_u64))
        multi.stop
      end
      (a.pushes + b.pushes).should eq 20
      a.pushes.should be > 0
      b.pushes.should be > 0
    end

    it "raises when the drawn destination can't start" do
      multi = LavinMQ::Shovel::MultiDestination.new(
        [ShovelSpecHelpers::FakeDestination.new(Exception.new("down"))] of LavinMQ::Shovel::Destination)
      expect_raises(Exception, "down") { multi.start }
      multi.started?.should be_false
    end

    it "rejects an empty list" do
      expect_raises(ArgumentError) { LavinMQ::Shovel::MultiDestination.new([] of LavinMQ::Shovel::Destination) }
    end
  end

  describe "Store.validate_config!" do
    it "checks the user's permissions on in-process endpoints" do
      with_amqp_server do |s|
        s.vhosts.create("other")
        user = s.users.create("limited", "pass")
        # write on the default exchange "" to publish to a queue
        s.users.add_permission("limited", "/", /^(allowed|$)/, /^(allowed|$)/, /^(allowed|$)/)
        config = ->(src : String, dest : String, q : String) {
          JSON.parse({"src-uri" => src, "src-queue" => q, "dest-uri" => dest, "dest-queue" => q}.to_json)
        }
        LavinMQ::Shovel::Store.validate_config!(config.call("amqp://", "amqp://", "allowed_q"), user)
        expect_raises(LavinMQ::Shovel::ConfigError) do
          LavinMQ::Shovel::Store.validate_config!(config.call("amqp://", "amqp://", "secret_q"), user)
        end
        expect_raises(LavinMQ::Shovel::ConfigError) do
          LavinMQ::Shovel::Store.validate_config!(config.call("amqp:///other", "amqp://", "allowed_q"), user)
        end
        # A remote broker checks the credentials in the URI itself
        LavinMQ::Shovel::Store.validate_config!(config.call("amqp://u:p@remote", "amqp://u:p@remote", "secret_q"), user)
      end
    end

    it "requires a source and a destination" do
      expect_raises(LavinMQ::Shovel::ConfigError) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(%({"src-uri": "amqp://", "dest-uri": "amqp://", "dest-queue": "q"})), nil)
      end
      expect_raises(LavinMQ::Shovel::ConfigError) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(%({"src-uri": "amqp://", "src-queue": "q", "dest-uri": "amqp://"})), nil)
      end
      LavinMQ::Shovel::Store.validate_config!(JSON.parse(%({"src-uri": "amqp://", "src-queue": "q", "dest-uri": "https://example.com/"})), nil)
    end

    it "rejects a non-positive dest-timeout" do
      expect_raises(LavinMQ::Shovel::ConfigError) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(%({"src-uri": "amqp://", "src-queue": "q", "dest-uri": "https://example.com/", "dest-timeout": 0})), nil)
      end
    end
  end
end
