require "./spec_helper"

describe "Memory pressure" do
  it "stops flow and refuses new connections" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish_confirm("m1").should be_true
        s.memory_pressure!
        s.control_flow!
        s.flow?.should be_false
        s.flow_reason.should eq "Server under memory pressure"
        expect_raises(AMQP::Client::Channel::ClosedException, /memory pressure/) do
          q.publish_confirm("m2")
        end
        expect_raises(Exception) do
          AMQP::Client.new(port: amqp_port(s)).connect
        end
      end
    end
  end

  it "does not refuse connections when disabled" do
    config = LavinMQ::Config.new
    config.memory_pressure_refuse_connections = false
    with_amqp_server(config: config) do |s|
      s.memory_pressure!
      s.control_flow!
      with_channel(s) do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
          ch.queue("mp_queue")
        end
      end
    end
  end

  it "holds until pressure is relieved" do
    with_amqp_server do |s|
      s.memory_pressure!
      3.times { s.control_flow! }
      s.flow?.should be_false
      s.memory_pressure_relieved!
      s.control_flow!
      s.flow?.should be_true
      with_channel(s) do |ch|
        ch.queue.publish_confirm("m1").should be_true
      end
    end
  end

  it "keeps flow stopped while disk is full after memory pressure is relieved" do
    LavinMQ::Config.instance.free_disk_min = Int64::MAX
    with_amqp_server do |s|
      s.update_system_metrics(nil)
      s.memory_pressure!
      s.control_flow!
      s.memory_pressure_relieved!
      s.control_flow!
      s.flow?.should be_false
      s.flow_reason.should eq "Server low on disk space"
    end
  ensure
    LavinMQ::Config.instance.free_disk_min = 0
  end

  it "sends connection.blocked and unblocked" do
    config = LavinMQ::Config.new
    config.memory_pressure_refuse_connections = false
    with_amqp_server(config: config) do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      blocked = Channel(String).new(1)
      unblocked = Channel(Nil).new(1)
      conn.on_blocked { |reason| blocked.send reason }
      conn.on_unblocked { unblocked.send nil }
      s.memory_pressure!
      s.control_flow!
      blocked.receive.should eq "Server under memory pressure"
      s.memory_pressure_relieved!
      s.control_flow!
      unblocked.receive
    ensure
      conn.try &.close
    end
  end

  it "doesn't deliver a stale blocked notification after unblocked" do
    with_amqp_server do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      events = Channel(Symbol).new(10)
      conn.on_blocked { events.send :blocked }
      conn.on_unblocked { events.send :unblocked }
      s.flow(false, "test")
      events.receive.should eq :blocked
      s.flow(true)
      events.receive.should eq :unblocked
      # a notifier for the earlier flow(false) that fell behind, e.g. stuck
      # writing to a slow client, reaching this client only now
      server_client = s.vhosts["/"].connections.first.as(LavinMQ::AMQP::Client)
      server_client.notify_flow
      select
      when event = events.receive
        fail "unexpected #{event}"
      when timeout(100.milliseconds)
      end
      conn.blocked?.should be_false
    ensure
      conn.try &.close
    end
  end

  it "ends unblocked after rapid flow changes" do
    with_amqp_server do |s|
      conn = AMQP::Client.new(port: amqp_port(s)).connect
      events = Channel(Symbol).new(100)
      conn.on_blocked { events.send :blocked }
      conn.on_unblocked { events.send :unblocked }
      10.times do
        s.flow(false, "test")
        s.flow(true)
      end
      received = [] of Symbol
      loop do
        select
        when event = events.receive
          received << event
        when timeout(100.milliseconds)
          break
        end
      end
      received.each_cons_pair { |a, b| a.should_not eq b }
      received.last?.try &.should eq :unblocked
      conn.blocked?.should be_false
    ensure
      conn.try &.close
    end
  end

  it "releases segment memory" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        10.times { q.publish "m" }
        s.release_memory
        q.get(no_ack: true).should_not be_nil
      end
    end
  end

  it "releases stream segment memory" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.prefetch 1
        q = ch.queue("mp_stream", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))
        10.times { q.publish_confirm "m" }
        s.release_memory
        msgs = Channel(String).new(10)
        q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset" => "first"})) do |msg|
          msgs.send msg.body_io.to_s
          msg.ack
        end
        10.times { msgs.receive.should eq "m" }
      end
    end
  end

  it "releases priority queue segment memory" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("mp_prio", args: AMQP::Client::Arguments.new({"x-max-priority" => 3}))
        4.times { |i| q.publish_confirm "m#{i}", props: AMQP::Client::Properties.new(priority: i.to_u8) }
        s.release_memory
        4.times { |i| q.get(no_ack: true).try(&.body_io.to_s).should eq "m#{3 - i}" }
      end
    end
  end

  it "releases the channel publish buffer" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish_confirm("x" * 100_000).should be_true
        server_ch = s.vhosts["/"].connections.first.as(LavinMQ::AMQP::Client).channels.first.as(LavinMQ::AMQP::Channel)
        server_ch.@next_msg_body_tmp.@capacity.should be >= 100_000
        s.release_memory
        q.publish_confirm("m").should be_true
        server_ch.@next_msg_body_tmp.@capacity.should be < 100_000
      end
    end
  end
end
