require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/clustering/controller"

private class SpecController < LavinMQ::Clustering::Controller
  property fake_leader_uri : String? = nil
  property? fake_leader = false

  def follow_leader_public
    follow_leader
  end

  private def current_leader_uri : String?
    @fake_leader_uri
  end

  private def leader? : Bool
    @fake_leader
  end
end

# Not yet waiting on leader changes when the first one comes in, so the etcd
# migration fiber is the one that receives it.
private class SlowFollowController < LavinMQ::Clustering::Controller
  @delayed = false

  private def follow(uri : String?) : Symbol?
    unless @delayed
      @delayed = true
      sleep 2.seconds
    end
    super
  end
end

private def free_port : Int32
  s = TCPServer.new("127.0.0.1", 0)
  s.local_address.port
ensure
  s.try &.close
end

# Controllers of a cluster, each with its own data dir and raft port. The
# block given to run is recorded so specs can see who's serving.
private class ControllerCluster
  getter controllers = Array(LavinMQ::Clustering::Controller).new
  getter serving = Channel(LavinMQ::Clustering::Controller).new(8)
  getter exits = Channel(Tuple(LavinMQ::Clustering::Controller, Int32)).new(8)
  getter dirs = Array(String).new

  def initialize(size : Int32, with_data = false, bootstrap : Int32? = nil, etcd : String? = nil, etcds : Array(String)? = nil,
                 slow_follow : Int32? = nil)
    ports = Array.new(size) { free_port }
    peers = ports.map { |p| "127.0.0.1:#{p}" }.join(',')
    ports.each do |port|
      dir = File.tempname("lavinmq", "controller-spec")
      Dir.mkdir_p dir
      File.write(File.join(dir, "users.json"), "[]") if with_data
      @dirs << dir
      config = LavinMQ::Config.new
      config.clustering_bootstrap = bootstrap == @dirs.size - 1
      (etcds.try(&.[@dirs.size - 1]) || etcd).try { |e| config.clustering_etcd_endpoints = e }
      config.data_dir = dir
      config.clustering = true
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = port
      config.clustering_raft_advertised_address = "127.0.0.1:#{port}"
      config.clustering_peers = peers
      config.clustering_password = "controller-spec"
      config.clustering_election_timeout = 300
      config.clustering_heartbeat_interval = 50
      config.clustering_port = free_port
      config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
      config.metrics_http_port = -1
      # Followers proxy client ports to the leader, let each pick its own
      config.amqp_port = config.http_port = config.mqtt_port = 0
      config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
      @controllers << (slow_follow == @dirs.size - 1 ? SlowFollowController.new(config) : LavinMQ::Clustering::Controller.new(config))
    end
  end

  def start(controller)
    spawn(name: "controller spec #{controller.id}") do
      controller.run { @serving.send controller }
    rescue ex : SpecExit
      @exits.send({controller, ex.code})
    end
  end

  def start_all
    @controllers.each { |c| start(c) }
  end

  def next_leader(timeout = 5.seconds) : LavinMQ::Clustering::Controller
    select
    when c = @serving.receive
      c
    when timeout(timeout)
      fail "no leader elected within #{timeout}"
    end
  end

  def close
    @controllers.each &.stop
    @dirs.each { |d| FileUtils.rm_rf d }
  end
end

private def with_controllers(size = 3, with_data = false, bootstrap : Int32? = nil, etcd : String? = nil, etcds : Array(String)? = nil,
                             slow_follow : Int32? = nil, &)
  cluster = ControllerCluster.new(size, with_data, bootstrap, etcd, etcds, slow_follow)
  yield cluster
ensure
  cluster.try &.close
end

# Answers the etcd v3 JSON gateway calls EtcdSeed makes, for the keys an
# etcd-coordinated cluster kept under the default "lavinmq" prefix. The
# election leader is modelled as a single candidate key, and changing it bumps
# the revision and wakes watches.
private class FakeEtcd
  property isr : Set(Int32)? = nil
  getter address : String
  getter leader : String? = nil
  getter watches_started = Atomic(Int32).new(0)
  @revision = 1i64
  @lock = Mutex.new
  @watchers = Array(Channel(Nil)).new

  def initialize
    @server = ::HTTP::Server.new do |ctx|
      body = JSON.parse(ctx.request.body.try(&.gets_to_end) || "{}")
      ctx.response.content_type = "application/json"
      if ctx.request.path == "/v3/watch"
        watch(ctx.response, body)
      else
        ctx.response.print(respond(ctx.request.path, body))
      end
    end
    addr = @server.bind_tcp("127.0.0.1", 0)
    @address = "127.0.0.1:#{addr.port}"
    spawn @server.listen
  end

  def leader=(uri : String?)
    watchers = @lock.synchronize do
      @leader = uri
      @revision += 1
      @watchers.dup.tap { @watchers.clear }
    end
    watchers.each { |w| w.send(nil) rescue nil }
  end

  private def watch(response, body)
    @watches_started.add(1)
    start = body.dig("create_request", "start_revision").as_s.to_i64
    wake = Channel(Nil).new(1)
    changed = @lock.synchronize do
      @watchers << wake unless @revision >= start
      @revision >= start
    end
    response.print({result: {header: {revision: @revision.to_s}, created: true}}.to_json)
    response.print('\n')
    response.flush
    wake.receive unless changed
    response.print({result: {events: [{type: "DELETE"}]}}.to_json)
    response.print('\n')
    response.flush
  end

  private def respond(path, body) : String
    case path
    when "/v3/kv/range"
      key = Base64.decode_string(body["key"].as_s)
      header = {revision: @lock.synchronize { @revision }.to_s}
      value = case key
              when "lavinmq/leader/"           then @leader
              when "lavinmq/isr"               then @isr.try &.map(&.to_s(36)).join(',')
              when "lavinmq/clustering_secret" then "etcd-secret"
              end
      if value
        {header: header, kvs: [{value: Base64.strict_encode(value)}]}.to_json
      else
        {header: header, count: "0"}.to_json
      end
    else
      {error: "unknown path #{path}"}.to_json
    end
  end

  def close
    @server.close
  end
end

describe LavinMQ::Clustering::EtcdSeed do
  it "reads the election leader, the ISR and the replication secret" do
    etcd = FakeEtcd.new
    seed = LavinMQ::Clustering::EtcdSeed.new(etcd.address, "lavinmq")
    seed.election.leader_uri.should be_nil
    seed.isr.should be_nil
    etcd.leader = "tcp://n1:5679"
    etcd.isr = Set{42, 4711}
    seed.election.leader_uri.should eq "tcp://n1:5679"
    seed.isr.should eq Set{42, 4711}
    seed.clustering_secret.should eq "etcd-secret"
  ensure
    etcd.try &.close
  end

  it "waits on a watch for the election to change" do
    etcd = FakeEtcd.new
    etcd.leader = "tcp://n1:5679"
    seed = LavinMQ::Clustering::EtcdSeed.new(etcd.address, "lavinmq")
    election = seed.election
    done = Channel(Nil).new(1)
    spawn do
      seed.wait_for_election_change(election.revision)
      done.send nil
    end
    wait_for { etcd.watches_started.get == 1 }
    select
    when done.receive
      fail "returned before the election changed"
    when timeout(200.milliseconds)
    end
    etcd.leader = nil # lease expired
    select
    when done.receive
    when timeout(1.second)
      fail "watch didn't return on the change"
    end
  ensure
    etcd.try &.close
  end

  it "doesn't miss a change between the read and the watch" do
    etcd = FakeEtcd.new
    etcd.leader = "tcp://n1:5679"
    seed = LavinMQ::Clustering::EtcdSeed.new(etcd.address, "lavinmq")
    election = seed.election
    etcd.leader = nil                                # changes before the watch is set up
    seed.wait_for_election_change(election.revision) # returns at once
  ensure
    etcd.try &.close
  end

  it "is interrupted by close" do
    etcd = FakeEtcd.new
    etcd.leader = "tcp://n1:5679"
    seed = LavinMQ::Clustering::EtcdSeed.new(etcd.address, "lavinmq")
    election = seed.election
    raised = Channel(Exception?).new(1)
    spawn do
      seed.wait_for_election_change(election.revision)
      raised.send nil
    rescue ex
      raised.send ex
    end
    wait_for { etcd.watches_started.get == 1 }
    seed.close
    select
    when ex = raised.receive
      ex.should be_a(LavinMQ::Clustering::EtcdSeed::Error)
    when timeout(1.second)
      fail "close didn't interrupt the watch"
    end
  ensure
    etcd.try &.close
  end

  it "tries the next endpoint when one is unreachable" do
    etcd = FakeEtcd.new
    etcd.leader = "tcp://n1:5679"
    seed = LavinMQ::Clustering::EtcdSeed.new("127.0.0.1:#{free_port},#{etcd.address}", "lavinmq")
    seed.election.leader_uri.should eq "tcp://n1:5679"
  ensure
    etcd.try &.close
  end

  it "raises when no endpoint is reachable" do
    seed = LavinMQ::Clustering::EtcdSeed.new("127.0.0.1:#{free_port}", "lavinmq")
    expect_raises(LavinMQ::Clustering::EtcdSeed::Error) { seed.isr }
  end
end

describe LavinMQ::Clustering::Controller do
  describe "migrating from etcd", tags: "slow" do
    it "follows the etcd leader, then elects an ISR member once its lease is gone" do
      etcd = FakeEtcd.new
      etcd_leader = "tcp://127.0.0.1:#{free_port}"
      etcd.leader = etcd_leader # the etcd-era leader, still serving
      with_controllers(with_data: true, etcd: etcd.address) do |cluster|
        cluster.start_all
        # Replicating from it like etcd-era followers, with its etcd secret
        wait_for do
          cluster.controllers.all? do |c|
            c.@repli_client.try(&.follows?(etcd_leader)) && c.@repli_password == "etcd-secret"
          end
        end
        select
        when c = cluster.serving.receive
          fail "#{c.id} was elected while the etcd leader held its lease"
        when timeout(1.second)
        end
        in_sync = cluster.controllers[1, 2]
        etcd.isr = in_sync.map(&.id).to_set
        etcd.leader = nil # the etcd leader stopped, its lease is gone
        leader = cluster.next_leader
        in_sync.should contain leader
        # The rest now replicate from the raft leader, with the raft password
        others = cluster.controllers.reject(leader)
        wait_for do
          others.all? { |o| o.@repli_client.try(&.follows?(leader.@advertised_uri)) && o.@repli_password == "controller-spec" }
        end
      end
    ensure
      etcd.try &.close
    end

    it "follows a raft leader elected while waiting out its own etcd lease" do
      # n0 still sees its previous incarnation's lease, n1 and n2 see it gone
      own_lease = FakeEtcd.new
      lease_gone = FakeEtcd.new
      with_controllers(with_data: true, etcds: [own_lease.address, lease_gone.address, lease_gone.address], slow_follow: 0) do |cluster|
        waiting = cluster.controllers[0]
        own_lease.leader = waiting.@advertised_uri
        lease_gone.isr = cluster.controllers[1, 2].map(&.id).to_set
        cluster.controllers[1, 2].each { |c| cluster.start(c) }
        leader = cluster.next_leader
        cluster.start(waiting)
        wait_for(5.seconds) { waiting.@repli_client.try(&.follows?(leader.@advertised_uri)) }
      end
    ensure
      own_lease.try &.close
      lease_gone.try &.close
    end

    it "waits for the only ISR member to come back" do
      etcd = FakeEtcd.new
      with_controllers(with_data: true, etcd: etcd.address) do |cluster|
        only = cluster.controllers[0]
        etcd.isr = Set{only.id}
        cluster.controllers[1, 2].each { |c| cluster.start(c) }
        select
        when c = cluster.serving.receive
          fail "#{c.id} was elected without being in the etcd ISR"
        when timeout(2.seconds)
        end
        cluster.start(only)
        cluster.next_leader.should eq only
      end
    ensure
      etcd.try &.close
    end
  end

  it "reports follower proxy bind failures without the generic unhandled exception log" do
    blocker = TCPServer.new("127.0.0.1", 0)
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.amqp_bind = "127.0.0.1"
      config.amqp_port = blocker.local_address.port
      config.http_port = 0
      config.mqtt_port = 0
      config.metrics_http_port = -1
      config.clustering_password = "secret"
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      controller = SpecController.new(config)
      controller.fake_leader_uri = "tcp://192.0.2.10:5679"

      Log.capture("lmq.clustering.controller", :fatal) do |logs|
        ex = expect_raises(SpecExit) { controller.follow_leader_public }
        ex.code.should eq 36
        logs.check(:fatal, /Could not bind to '127\.0\.0\.1:#{blocker.local_address.port}'/)
        logs.entry.to_s.should_not contain "Unhandled exception while following leader"
      end
    end
  ensure
    blocker.try &.close
  end

  it "stops following once this node is the leader" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_password = "secret"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      controller = SpecController.new(config)
      controller.fake_leader_uri = config.clustering_advertised_uri
      controller.fake_leader = true
      controller.follow_leader_public # returns instead of blocking or exiting
    end
  end

  it "exits when another node advertises the same URI" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_password = "secret"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      controller = SpecController.new(config)
      controller.fake_leader_uri = config.clustering_advertised_uri
      ex = expect_raises(SpecExit) { controller.follow_leader_public }
      ex.code.should eq 36
    end
  end

  it "elects a single leader and fails over", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      first.stop
      second = cluster.next_leader
      second.should_not eq first
      select
      when extra = cluster.serving.receive
        fail "two leaders serving: #{extra.id}"
      when timeout(500.milliseconds)
      end
    end
  end

  it "hands over leadership on shutdown faster than an election timeout", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      started = Time.instant
      spawn { first.stop }
      cluster.next_leader(timeout: 2.seconds)
      (Time.instant - started).should be < 300.milliseconds
    end
  end

  it "only fails over to nodes in the ISR", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      # A new leader's ISR is just itself until followers have synced from it
      others = cluster.controllers.reject(first)
      in_sync = others.first
      first.coordinator.update_isr(Set{first.id, in_sync.id})
      first.stop
      cluster.next_leader.should eq in_sync
    end
  end

  it "doesn't fail over when no other node is in the ISR", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.stop
      select
      when c = cluster.serving.receive
        fail "#{c.id} was elected without being in the ISR"
      when timeout(2.seconds)
      end
    end
  end

  it "doesn't elect nodes with data but no raft state, unless bootstrapped", tags: "slow" do
    with_controllers(with_data: true) do |cluster|
      cluster.start_all
      select
      when c = cluster.serving.receive
        fail "#{c.id} was elected without knowing if its data is current"
      when timeout(1.second)
      end
    end
    with_controllers(with_data: true, bootstrap: 1) do |cluster|
      cluster.start_all
      cluster.next_leader.should eq cluster.controllers[1]
    end
  end

  it "doesn't exit with an error when losing leadership while shutting down", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      first.stopping
      cluster.controllers.reject(first).each(&.stop)
      select
      when exit = cluster.exits.receive
        fail "exited with #{exit[1]} during a graceful shutdown"
      when timeout(2.seconds)
      end
    end
  end

  it "exits when it loses leadership", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      # Cut the leader off from its peers: it has to step down on its own
      cluster.controllers.reject(first).each(&.stop)
      select
      when exit = cluster.exits.receive
        exit[0].should eq first
        exit[1].should eq 3
      when timeout(5.seconds)
        fail "leader cut off from the majority kept serving"
      end
    end
  end
end
