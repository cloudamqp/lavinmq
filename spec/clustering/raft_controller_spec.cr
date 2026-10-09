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

# Records the exits of the watchdogs, which run in fibers of their own.
private class ExitRecordingController < LavinMQ::Clustering::RaftController
  def initialize(config : LavinMQ::Config, @exits : Channel(ControllerExit))
    super(config)
  end

  private def watch_startup(started : Channel(Nil)) : Nil
    super
  rescue ex : SpecExit
    @exits.send({self, ex.code})
  end

  private def watch_demotion(done : Channel(Nil)) : Nil
    super
  rescue ex : SpecExit
    @exits.send({self, ex.code})
  end
end

private alias ControllerDemotion = Tuple(LavinMQ::Clustering::RaftController, Bool)

private def receive_within(channel : Channel(T), span : Time::Span) : T? forall T
  select
  when value = channel.receive
    value
  when timeout(span)
    nil
  end
end

private def should_not_have_exited(cluster) : Nil
  if exit = receive_within(cluster.exits, 0.seconds)
    fail "exited with #{exit[1]}"
  end
end

# Controllers of a cluster, each with its own data dir and raft port. The
# block given to run is recorded so specs can see who's serving.
private class ControllerCluster
  getter controllers = Array(LavinMQ::Clustering::RaftController).new
  getter serving = Channel(LavinMQ::Clustering::RaftController).new(8)
  getter exits = Channel(ControllerExit).new(8)
  # Each time a leader stops serving, and whether it lost leadership
  getter demotions = Channel(ControllerDemotion).new(8)
  getter dirs = Array(String).new
  getter configs = Array(LavinMQ::Config).new
  # With *replication*, a leader also serves its data to followers, as the
  # Launcher does, so followers sync and the ISR follows.
  getter servers = Hash(LavinMQ::Clustering::RaftController, LavinMQ::Clustering::Server).new
  # Replication listeners of the nodes that haven't led yet, by config index
  @replication_listeners = Hash(Int32, TCPServer).new

  def initialize(size : Int32, bootstrap : Int32? = 0, @replication = false, @election_timeout = 300)
    ports = Array.new(size) { free_port }
    # A port picked by free_port can be taken by another process before it's
    # bound, so don't let a node join someone else's cluster
    @password = Random::Secure.hex(16)
    seeds = ports.map { |p| "127.0.0.1:#{p}" }.join(',')
    ports.each do |port|
      add_node(port, seeds, bootstrap == @dirs.size)
    end
  end

  # A node that isn't started yet. Doesn't bootstrap unless told to.
  def add_node(port : Int32, seeds : String, bootstrap = false) : LavinMQ::Clustering::RaftController
    dir = File.tempname("lavinmq", "controller-spec")
    Dir.mkdir_p dir
    @dirs << dir
    config = LavinMQ::Config.new
    @configs << config
    config.clustering_bootstrap = bootstrap
    config.data_dir = dir
    config.clustering = true
    config.clustering_bind = "127.0.0.1"
    config.clustering_raft_port = port
    config.clustering_raft_advertised_address = "127.0.0.1:#{port}"
    config.clustering_seeds = seeds
    config.clustering_secret = @password
    config.clustering_election_timeout = @election_timeout
    config.clustering_heartbeat_interval = @election_timeout // 6
    # Bound now and kept until the node leads: a port picked and bound only
    # after the election could be taken by then, e.g. as the source port of
    # one of the nodes' raft connections
    replication_listener = TCPServer.new("127.0.0.1", 0)
    @replication_listeners[@configs.size - 1] = replication_listener
    config.clustering_port = replication_listener.local_address.port
    config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
    config.metrics_http_port = -1
    # Followers proxy client ports to the leader, let each pick its own
    config.amqp_port = config.http_port = config.mqtt_port = 0
    config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
    ExitRecordingController.new(config, @exits).tap do |c|
      register(c)
      @controllers << c
    end
  end

  # Stops serving replication when a leader steps down, as the Launcher does
  private def register(controller : LavinMQ::Clustering::RaftController) : Nil
    controller.on_demote do |hand_over|
      hand_over.try &.call
      @servers.delete(controller).try &.close
      @demotions.send({controller, hand_over.nil?})
    end
  end

  def address(controller : LavinMQ::Clustering::RaftController) : String
    @configs[@controllers.index!(controller)].clustering_raft_advertised_address.not_nil!
  end

  # Starts a new controller with the config of a stopped one, like a
  # supervisor restarting the process.
  def restart(controller : LavinMQ::Clustering::RaftController) : LavinMQ::Clustering::RaftController
    index = @controllers.index!(controller)
    @controllers[index] = fresh = ExitRecordingController.new(@configs[index], @exits)
    register(fresh)
    start(fresh)
    fresh
  end

  # *startup* runs as the leader's startup, after it's reported as serving.
  def start(controller, startup : Proc(Nil) = -> { })
    spawn(name: "controller spec #{controller.id}") do
      controller.run do
        serve_replication(controller) if @replication
        @serving.send controller
        startup.call
      end
    rescue ex : SpecExit
      @exits.send({controller, ex.code})
    rescue ex
      # Would otherwise only show as no leader being elected
      STDERR.puts "controller spec #{controller.id} failed: #{ex.inspect_with_backtrace}"
      raise ex
    end
  end

  private def serve_replication(controller)
    config = @configs[@controllers.index!(controller)]
    server = LavinMQ::Clustering::Server.new(config, controller.coordinator, controller.id)
    @servers[controller] = server
    tcp = @replication_listeners.delete(@configs.index!(config)) ||
          TCPServer.new(config.clustering_bind.not_nil!, config.clustering_port) # a restarted leader
    spawn(name: "replication spec #{controller.id}") { server.listen(tcp) }
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
    @replication_listeners.each_value &.close
    @servers.each_value &.close
    @controllers.each &.stop
    @dirs.each { |d| FileUtils.rm_rf d }
  end
end

private def with_controllers(size = 3, bootstrap : Int32? = 0, replication = false, election_timeout = 300, &)
  cluster = ControllerCluster.new(size, bootstrap, replication, election_timeout)
  yield cluster
ensure
  cluster.try &.close
end

# Serves *controller*'s metrics as the Launcher sets it up, yields a scrape
private def with_metrics_of(controller : LavinMQ::Clustering::RaftController, &)
  metrics = LavinMQ::HTTP::MetricsServer.new(raft: controller.node)
  addr = metrics.bind_tcp("127.0.0.1", 0)
  controller.metrics_server = metrics
  spawn metrics.listen
  yield -> { HTTP::Client.get("http://#{addr}/metrics").body rescue "" }
ensure
  metrics.try &.close
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

  it "keeps raft running while the default execution context is busy", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      leader = cluster.next_leader
      leader.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      follower = cluster.controllers.find! { |c| c != leader }
      term = leader.node.status.not_nil!.term
      busy_until = Time.instant + 300.milliseconds * 5
      # Watches from a thread of its own how long the follower's raft node
      # takes to answer, and how long ago it heard from the leader
      worst = Channel(Time::Span).new(1)
      Fiber::ExecutionContext::Isolated.new("raft observer") do
        max = Time::Span.zero
        until Time.instant >= busy_until
          asked = Time.instant
          status = follower.node.status.not_nil!
          max = {max, Time.instant - asked, status.leader_heard_ago || Time::Span.zero}.max
          sleep 20.milliseconds
        end
        worst.send max
      end
      # Hog every default context thread for several election timeouts,
      # without yielding, like a broker busy with heavy work
      hogs = WaitGroup.new
      Fiber::ExecutionContext.default.capacity.times do
        hogs.spawn do
          until Time.instant >= busy_until
          end
        end
      end
      hogs.wait
      worst.receive.should be < 300.milliseconds
      leader.node.status.not_nil!.role.leader?.should be_true
      cluster.controllers.each(&.node.status.not_nil!.term.should(eq(term)))
    end
  end

  it "hands over leadership on shutdown faster than an election timeout", tags: "slow" do
    # Generous, the handover takes several fsyncs that can be slow on CI
    with_controllers(election_timeout: 1000) do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      started = Time.instant
      spawn { first.stop }
      cluster.next_leader(timeout: 2.seconds)
      (Time.instant - started).should be < 1.second
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

  it "reports raft metrics without a leader and while following", tags: "slow" do
    # Nodes without raft state can't elect a leader unless one may bootstrap.
    # All nodes start at once, a node started later could find its port taken.
    with_controllers(bootstrap: nil) do |cluster|
      with_metrics_of(cluster.controllers[0]) do |scrape|
        cluster.start_all
        peer = cluster.controllers[1].id.to_s(36)
        wait_for { scrape.call.includes? %(lavinmq_raft_peer_connected{peer="#{peer}"} 1) }
        body = scrape.call
        body.should contain "lavinmq_raft_has_leader 0"
        body.should contain "lavinmq_raft_is_leader 0"
        body.should_not contain "lavinmq_raft_leader_last_contact_seconds"
        body.should_not contain "lavinmq_cluster_received_bytes_total"
      end
    end
    with_controllers(replication: true) do |cluster|
      with_metrics_of(cluster.controllers[1]) do |scrape|
        cluster.start_all
        cluster.next_leader.should eq cluster.controllers[0]
        # Along with the replication client's
        wait_for { scrape.call.includes? "lavinmq_cluster_received_bytes_total" }
        body = scrape.call
        body.should contain "lavinmq_raft_has_leader 1"
        body.should contain "lavinmq_raft_is_leader 0"
        body.should contain "lavinmq_raft_leader_changes_seen_total 1"
        body.should contain "lavinmq_raft_leader_last_contact_seconds"
        body.should contain "# TYPE lavinmq_raft_storage_save_duration_seconds histogram"
        body.should match /^lavinmq_raft_storage_save_duration_seconds_bucket\{le="0.001"\} \d+$/m
        body.should match /^lavinmq_raft_storage_save_duration_seconds_bucket\{le="\+Inf"\} [1-9]\d*$/m
        body.should match /^lavinmq_raft_storage_save_duration_seconds_count [1-9]\d*$/m
        body.should match /^lavinmq_raft_storage_save_duration_seconds_sum \d/m
      end
    end
  end

  it "doesn't step down when losing leadership while shutting down", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      first.stopping
      cluster.controllers.reject(first).each(&.stop)
      if demotion = receive_within(cluster.demotions, 2.seconds)
        fail "stepped down to follow during a graceful shutdown (lost: #{demotion[1]})"
      end
      should_not_have_exited(cluster)
    end
  end

  it "stops serving when it loses leadership during a startup that writes the ISR", tags: "slow" do
    with_controllers do |cluster|
      # A replicated write during startup fails once leadership is lost
      cluster.start_all(-> {
        wait_for(5.seconds) { cluster.controllers.none?(&.node.serving.value) }
        raise LavinMQ::Clustering::RaftCoordinator::StaleLeadership.new
      })
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      cluster.controllers.reject(first).each(&.stop)
      receive_within(cluster.demotions, 5.seconds).should eq({first, true})
      should_not_have_exited(cluster)
    end
  end

  it "exits when it loses leadership during a startup that hangs", tags: "slow" do
    with_controllers do |cluster|
      cluster.controllers.each &.demotion_timeout = 500.milliseconds
      cluster.start_all(-> { sleep })
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      cluster.controllers.reject(first).each(&.stop)
      select
      when exit = cluster.exits.receive
        exit.should eq({first, 3})
      when timeout(5.seconds)
        fail "leader cut off from the majority during startup kept running"
      end
    end
  end

  it "stops serving and follows when it loses leadership", tags: "slow" do
    with_controllers do |cluster|
      cluster.start_all
      first = cluster.next_leader
      first.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      # Cut the leader off from its peers: it has to step down on its own
      others = cluster.controllers.reject(first)
      others.each(&.stop)
      receive_within(cluster.demotions, 5.seconds).should eq({first, true})
      should_not_have_exited(cluster)
      first.node.leader?.should be_false
      # When its peers are back it can be elected again, in the same process
      others.each { |c| cluster.restart(c) }
      wait_for(10.seconds) { cluster.controllers.any?(&.node.serving.value) }
      should_not_have_exited(cluster)
    end
  end

  it "relocates a replica: learner, promotion, handover and removal", tags: "slow" do
    with_controllers(replication: true) do |cluster|
      cluster.start_all
      a = cluster.next_leader
      a_addr = cluster.address(a)
      wait_for(10.seconds) { a.node.committed_isr == cluster.controllers.map(&.id).to_set }

      # A new node starts with an existing member as its seed. It doesn't
      # campaign, and doesn't get anything until it's added.
      port = free_port
      d = cluster.add_node(port, a_addr)
      cluster.start(d)
      d_addr = cluster.address(d)
      sleep 100.milliseconds
      d.node.leader?.should be_false
      d.node.leader_uri.should be_nil

      a.node.add_learner(d_addr).should be_nil
      a.node.membership.not_nil!.learners.should eq Set{d.id}
      # It gets the broker data from the leader and ends up in the ISR
      wait_for(10.seconds) { a.node.committed_isr.try(&.includes?(d.id)) }
      a.node.membership.not_nil!.voters.size.should eq 3

      deadline = Time.instant + 10.seconds
      while error = a.node.promote(d.id)
        error.should(be_a(LavinMQ::Clustering::Raft::MembershipError))
        fail "not promoted: #{error.message}" if Time.instant > deadline
        sleep 50.milliseconds
      end
      a.node.membership.not_nil!.voters.should contain(d.id)

      a.request_transfer("127.0.0.1:1").should be_a String # not a member
      plan = a.request_transfer(d_addr).as(LavinMQ::Clustering::RaftController::Transfer)
      plan.target.should eq d.id
      plan.address.should eq d_addr
      # A second request before the step down can't override the accepted one
      a.request_transfer("127.0.0.1:1").as(String).should contain "already in progress"
      a.request_transfer(d_addr).as(String).should contain "already in progress"
      a.step_down(plan)
      cluster.next_leader(10.seconds).should eq d
      cluster.demotions.receive.should eq({a, false})

      # The old leader continues as a follower, in the same process, and
      # replicates from the new one
      wait_for(10.seconds) { d.node.committed_isr.try(&.includes?(a.id)) }
      cluster.servers[d].all_followers.map(&.id).should contain(a.id)

      d.node.remove_member(a.id).should be_nil
      d.node.remove_member(a.id).should eq LavinMQ::Clustering::Raft::MembershipError::UnknownMember
      d.node.committed_isr.not_nil!.should_not contain(a.id)
      d.node.membership.not_nil!.members.should_not contain(a.id)

      # It's told, disconnected and refused when it comes back
      wait_for(10.seconds) { !a.node.self_member? }
      wait_for(10.seconds) { cluster.servers[d].all_followers.none? { |f| f.id == a.id } }
      sleep 2.5.seconds # a few reconnect attempts
      cluster.servers[d].all_followers.none? { |f| f.id == a.id }.should be_true
      d.node.committed_isr.not_nil!.should_not contain(a.id)
    end
  end

  it "refuses to hand over leadership to a voter that stopped answering" do
    with_controllers do |cluster|
      cluster.start_all
      leader = cluster.next_leader
      leader.coordinator.update_isr(cluster.controllers.map(&.id).to_set)
      gone, other = cluster.controllers.reject(leader)
      gone.stop
      sleep 500.milliseconds # longer than the election timeout
      leader.request_transfer(cluster.address(gone)).as(String).should contain "answered"
      plan = leader.request_transfer.as(LavinMQ::Clustering::RaftController::Transfer)
      plan.target.should eq other.id
    end
  end

  it "takes a follower back at a new raft address with its data dir", tags: "slow" do
    with_controllers(replication: true) do |cluster|
      cluster.start_all
      a = cluster.next_leader
      wait_for(10.seconds) { a.node.committed_isr == cluster.controllers.map(&.id).to_set }
      b = cluster.controllers.find! { |c| c != a }
      b.stop
      port = free_port
      config = cluster.configs[cluster.controllers.index!(b)]
      config.clustering_raft_port = port
      config.clustering_raft_advertised_address = "127.0.0.1:#{port}"
      cluster.restart(b)
      wait_for(10.seconds) do
        m = a.node.membership
        !m.nil? && m.addresses[b.id]? == "127.0.0.1:#{port}" && m.voters.includes?(b.id)
      end
      wait_for(10.seconds) { a.node.committed_isr.try(&.includes?(b.id)) }
    end
  end
end
