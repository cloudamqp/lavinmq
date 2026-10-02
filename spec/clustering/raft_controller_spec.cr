require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/clustering/controller"

private class SpecController < LavinMQ::Clustering::RaftController
  property fake_leader_uri : String? = nil
  property? fake_leader = false
  getter fake_serving = BoolChannel.new(false)

  def follow_leader_public
    follow_leader
  end

  private def serving : BoolChannel
    @fake_serving
  end

  private def current_leader_uri : String?
    @fake_leader_uri
  end

  private def leader? : Bool
    @fake_leader
  end
end

private def free_port : Int32
  s = TCPServer.new("127.0.0.1", 0)
  s.local_address.port
ensure
  s.try &.close
end

private alias ControllerExit = Tuple(LavinMQ::Clustering::RaftController, Int32)

# Records the exit on leadership loss, which happens in its own fiber.
private class ExitRecordingController < LavinMQ::Clustering::RaftController
  def initialize(config : LavinMQ::Config, @exits : Channel(ControllerExit))
    super(config)
  end

  private def exit_on_leadership_loss : Nil
    super
  rescue ex : SpecExit
    @exits.send({self, ex.code})
  end
end

# Controllers of a cluster, each with its own data dir and raft port. The
# block given to run is recorded so specs can see who's serving.
private class ControllerCluster
  getter controllers = Array(LavinMQ::Clustering::RaftController).new
  getter serving = Channel(LavinMQ::Clustering::RaftController).new(8)
  getter exits = Channel(ControllerExit).new(8)
  getter dirs = Array(String).new

  def initialize(size : Int32, bootstrap : Int32? = 0)
    ports = Array.new(size) { free_port }
    peers = ports.map { |p| "127.0.0.1:#{p}" }.join(',')
    ports.each do |port|
      dir = File.tempname("lavinmq", "controller-spec")
      Dir.mkdir_p dir
      @dirs << dir
      config = LavinMQ::Config.new
      config.clustering_bootstrap = bootstrap == @dirs.size - 1
      config.data_dir = dir
      config.clustering = true
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = port
      config.clustering_raft_advertised_address = "127.0.0.1:#{port}"
      config.clustering_peers = peers
      config.clustering_secret = "controller-spec"
      config.clustering_election_timeout = 300
      config.clustering_heartbeat_interval = 50
      config.clustering_port = free_port
      config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
      config.metrics_http_port = -1
      # Followers proxy client ports to the leader, let each pick its own
      config.amqp_port = config.http_port = config.mqtt_port = 0
      config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
      @controllers << ExitRecordingController.new(config, @exits)
    end
  end

  # *startup* runs as the leader's startup, after it's reported as serving.
  def start(controller, startup : Proc(Nil) = -> { })
    spawn(name: "controller spec #{controller.id}") do
      controller.run do
        @serving.send controller
        startup.call
      end
    rescue ex : SpecExit
      @exits.send({controller, ex.code})
    end
  end

  def start_all(startup : Proc(Nil) = -> { })
    @controllers.each { |c| start(c, startup) }
  end

  def next_leader(timeout = 5.seconds) : LavinMQ::Clustering::RaftController
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

private def with_controllers(size = 3, bootstrap : Int32? = 0, &)
  cluster = ControllerCluster.new(size, bootstrap)
  yield cluster
ensure
  cluster.try &.close
end

describe LavinMQ::Clustering::RaftController do
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
      config.clustering_secret = "secret"
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
      config.clustering_secret = "secret"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      controller = SpecController.new(config)
      controller.fake_leader_uri = config.clustering_advertised_uri
      controller.fake_leader = true
      controller.fake_serving.set(true)
      controller.follow_leader_public # returns instead of blocking or exiting
    end
  end

  it "keeps following when leadership is lost before serving" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_secret = "secret"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      controller = SpecController.new(config)
      controller.fake_leader_uri = config.clustering_advertised_uri
      controller.fake_leader = true
      returned = Channel(Nil).new
      Log.capture("lmq.clustering.controller", :warn) do |logs|
        spawn do
          controller.follow_leader_public
          returned.close
        end
        select
        when returned.receive?
          fail "stopped following while not serving"
        when timeout(100.milliseconds)
        end
        controller.fake_leader = false
        controller.fake_leader_uri = nil
        controller.node.leader_changed.send nil
        wait_for { logs.check(:warn, "No leader available") rescue nil }
      end
      controller.stop
      returned.receive?
    end
  end

  it "exits when another node advertises the same URI" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_secret = "secret"
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

  it "doesn't elect nodes without raft state, unless bootstrapped", tags: "slow" do
    # Even with empty data dirs: a majority of new nodes could otherwise
    # outvote a node with data
    with_controllers(bootstrap: nil) do |cluster|
      cluster.start_all
      select
      when c = cluster.serving.receive
        fail "#{c.id} was elected without knowing if its data is current"
      when timeout(1.second)
      end
    end
    with_controllers(bootstrap: 1) do |cluster|
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

  it "exits when it loses leadership during startup", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all(-> { sleep }) # e.g. stuck in a replicated write
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      cluster.controllers.reject(first).each(&.stop)
      select
      when exit = cluster.exits.receive
        exit[0].should eq first
        exit[1].should eq 3
      when timeout(5.seconds)
        fail "leader cut off from the majority during startup kept running"
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
