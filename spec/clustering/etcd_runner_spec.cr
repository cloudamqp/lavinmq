require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/launcher"
require "../../src/lavinmq/clustering/runner"

private class SelfLeaderEtcd < LavinMQ::Etcd
  getter observed = Channel(Nil).new(1)

  def initialize(@uri : String)
    super("localhost:1")
  end

  def elect_listen(_name, &)
    @observed.send nil
    yield @uri
  end
end

private class SelfLeaderRunner < LavinMQ::Clustering::EtcdRunner
  def follow_leader_public
    follow_leader
  end

  def mark_elected_for_spec
    @elected_leader.set(true)
  end
end

private class ProxyBindEtcd < LavinMQ::Etcd
  def initialize(@leader_uri : String)
    super("localhost:1")
  end

  def elect_listen(_name, &)
    yield @leader_uri
  end

  def get(_key) : String?
    "secret"
  end
end

# Waits in #get, as an etcd slow to answer, until *gate* is closed
private class SlowSecretEtcd < LavinMQ::Etcd
  getter getting = Channel(Nil).new(1)
  getter gate = Channel(Nil).new

  def initialize(@leader_uri : String)
    super("localhost:1")
  end

  def elect_listen(_name, &)
    yield @leader_uri
  end

  def get(_key) : String?
    @getting.send nil
    @gate.receive?
    "secret"
  end
end

# No follower monitor: a replication client set with #following_for_spec is
# left to #run, as when this node is elected before the monitor sees it
private class UnmonitoredRunner < LavinMQ::Clustering::EtcdRunner
  def following_for_spec(client : LavinMQ::Clustering::Client) : Nil
    @repli_client.set(client)
  end

  def lease_for_spec=(lease : LavinMQ::Etcd::Lease)
    @lease.set(lease)
  end

  private def follow_leader_changes
  end
end

# Revoking a lease fails, as when etcd is unreachable
private class FailingRevokeEtcd < LavinMQ::Etcd
  def initialize
    super("localhost:1")
  end

  def lease_keepalive(id) : Int32
    10
  end

  def lease_revoke(id) : Nil
    raise LavinMQ::Etcd::Error.new("etcd unreachable")
  end
end

# Records the exit code instead of exiting, from whichever fiber exits, and
# parks that fiber like a real exit would
private class ExitRecordingRunner < LavinMQ::Clustering::EtcdRunner
  getter exit_codes = Channel(Int32).new(1)

  def exit(code : Int32) : NoReturn
    @exit_codes.send code
    loop { sleep 1.hour }
  end

  # Whether the leader_lost hook from `lease_spec_config` has run
  def leader_lost_hook_ran? : Bool
    File.exists?(File.join(@config.data_dir, "leader_lost"))
  end
end

private def lease_spec_config(data_dir : String, port : Int32, ttl = 10) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering_etcd_endpoints = "localhost:12379"
  config.clustering_advertised_uri = "tcp://localhost:#{port}"
  config.clustering_etcd_lease_ttl = ttl
  # A follower's proxies bind them
  config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
  config.amqp_port = config.mqtt_port = config.http_port = 0
  config.clustering_port = 0 # a leader listens for followers on it
  # Quiet when a runner stopped during cleanup runs it after its dir is gone
  config.clustering_on_leader_lost = "touch #{File.join(data_dir, "leader_lost")} 2>/dev/null"
  config
end

# Starts a leader and a candidate queued in the election behind it
private def with_leader_and_candidate(name : String, candidate_ttl = 10, &)
  with_datadir do |leader_dir|
    with_datadir do |candidate_dir|
      leader = ExitRecordingRunner.new(lease_spec_config(leader_dir, 5686))
      candidate = ExitRecordingRunner.new(lease_spec_config(candidate_dir, 5687, ttl: candidate_ttl))
      begin
        etcd = LavinMQ::Etcd.new("localhost:12379")
        spawn(name: "#{name} leader") { leader.run { } }
        wait_for { etcd.get("lavinmq/leader/#{leader.id.to_s(16)}") }
        spawn(name: "#{name} candidate") { candidate.run { } }
        wait_for { etcd.get("lavinmq/leader/#{candidate.id.to_s(16)}") }
        yield leader, candidate, etcd
      ensure
        leader.shutdown { } rescue nil
        leader.close
        candidate.shutdown { } rescue nil
        candidate.close
      end
    end
  end
end

# Exit code of *runner* within *within*, or nil
private def exit_code(runner : ExitRecordingRunner, within : Time::Span) : Int32?
  select
  when code = runner.exit_codes.receive
    code
  when timeout(within)
  end
end

# Test-only seam: drive the SIGTERM shutdown path without raising the signal.
class LavinMQ::Launcher
  def shutdown_for_spec
    shutdown_server
  end
end

private def with_clustered_launcher_config(data_dir : String, &)
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering = true
  config.clustering_etcd_endpoints = "localhost:12379"
  config.clustering_advertised_uri = "tcp://localhost:5681"
  config.clustering_port = 0
  config.amqp_port = -1
  config.mqtt_port = -1
  config.http_port = -1
  config.metrics_http_port = 0
  control_unix_path = LavinMQ::Config.instance.control_unix_path
  LavinMQ::Config.instance.control_unix_path = File.join(data_dir, "control.sock")
  yield config
ensure
  LavinMQ::Config.instance.control_unix_path = control_unix_path if control_unix_path
end

describe LavinMQ::Clustering::EtcdRunner do
  # Its proxies would hold the client ports the broker binds next
  it "makes no replication client once elected while it waited for the secret" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
      config.amqp_port = config.http_port = config.mqtt_port = 0
      config.metrics_http_port = -1
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      etcd = SlowSecretEtcd.new("tcp://192.0.2.10:5679")
      runner = SelfLeaderRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      done = Channel(Nil).new
      spawn(name: "follower monitor") do
        runner.follow_leader_public
        done.close
      end
      etcd.getting.receive
      runner.mark_elected_for_spec
      etcd.gate.close
      select
      when done.receive?
      when timeout(5.seconds)
        fail "the follower monitor didn't return"
      end
      runner.@repli_client.get.should be_nil
    ensure
      if c = runner
        c.@repli_client.get.try &.close
      end
    end
  end

  it "makes no replication client once stopped while it waited for the secret" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
      config.amqp_port = config.http_port = config.mqtt_port = 0
      config.metrics_http_port = -1
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      etcd = SlowSecretEtcd.new("tcp://192.0.2.10:5679")
      runner = SelfLeaderRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      done = Channel(Nil).new
      spawn(name: "follower monitor") do
        runner.follow_leader_public
        done.close
      end
      etcd.getting.receive
      runner.shutdown { }
      runner.close
      etcd.gate.close
      select
      when done.receive?
      when timeout(5.seconds)
        fail "the follower monitor didn't return"
      end
      runner.@repli_client.get.should be_nil
    ensure
      if c = runner
        c.@repli_client.get.try &.close
      end
    end
  end

  # The SIGTERM handler would never get to exit
  it "stops when releasing its lease fails" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      etcd = FailingRevokeEtcd.new
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      runner.lease_for_spec = LavinMQ::Etcd::Lease.grant(etcd, 1i64, 10)
      runner.shutdown { }
      runner.close
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
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      etcd = ProxyBindEtcd.new("tcp://192.0.2.10:5679")
      coordinator = LavinMQ::Clustering::EtcdCoordinator.new(config, etcd)
      runner = SelfLeaderRunner.new(config, etcd, coordinator)

      Log.capture("lmq.clustering.runner", :fatal) do |logs|
        ex = expect_raises(SpecExit) { runner.follow_leader_public }
        ex.code.should eq 36
        logs.check(:fatal, /Could not bind to '127\.0\.0\.1:#{blocker.local_address.port}'/)
        logs.entry.to_s.should_not contain "Unhandled exception while following leader"
      end
    end
  ensure
    blocker.try &.close
  end
end

describe LavinMQ::Clustering::EtcdRunner, tags: "etcd" do
  add_etcd_around_each

  it "will failover", tags: "slow" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/failover1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5681"
    config1.clustering_port = 5681
    # A follower's proxies forward to the leader's ports, on this host the
    # follower's own when they're the same, until out of file descriptors
    config1.amqp_port = config1.mqtt_port = config1.http_port = 0
    runner1 = LavinMQ::Clustering::EtcdRunner.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/failover2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5682"
    config2.clustering_port = 5682
    config2.amqp_port = config2.mqtt_port = config2.http_port = 0
    runner2 = LavinMQ::Clustering::EtcdRunner.new(config2)

    listen = Channel(String?).new
    spawn(name: "etcd elect leader spec") do
      etcd = LavinMQ::Etcd.new("localhost:12379")
      etcd.elect_listen("lavinmq/leader") do |value|
        listen.send value
      end
    rescue SpecExit
      # expect this when etcd nodes are terminated
    end
    sleep 0.5.seconds
    spawn(name: "failover1") do
      runner1.run { }
    rescue SpecExit
    end
    spawn(name: "failover2") do
      runner2.run { }
    rescue SpecExit
    end
    sleep 0.1.seconds
    leader = listen.receive
    case leader
    when .try &.starts_with?(config1.clustering_advertised_uri.to_s)
      runner1.shutdown { }
      runner1.close
      listen.receive.to_s.should start_with config2.clustering_advertised_uri.to_s
      sleep 0.1.seconds
      runner2.shutdown { }
      runner2.close
    when .try &.starts_with?(config2.clustering_advertised_uri.to_s)
      runner2.shutdown { }
      runner2.close
      listen.receive.to_s.should start_with config1.clustering_advertised_uri.to_s
      sleep 0.1.seconds
      runner1.shutdown { }
      runner1.close
    else fail("no leader elected")
    end
  ensure
    runner1.try { |r| r.shutdown { } } rescue nil
    runner1.try &.close
    runner2.try { |r| r.shutdown { } } rescue nil
  end

  it "steps down if it wins the election while not in the ISR" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/isr-stepdown1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5683"
    # A follower's proxies would bind the default ports, and forward to the
    # leader's, the same ones on this host, until out of file descriptors
    config1.amqp_port = config1.mqtt_port = config1.http_port = 0
    FileUtils.rm_rf config1.data_dir
    runner1 = LavinMQ::Clustering::EtcdRunner.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/isr-stepdown2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5684"
    config2.amqp_port = config2.mqtt_port = config2.http_port = 0
    FileUtils.rm_rf config2.data_dir
    runner2 = LavinMQ::Clustering::EtcdRunner.new(config2)

    etcd = LavinMQ::Etcd.new("localhost:12379")
    leader1 = Channel(Nil).new
    spawn(name: "elect listen spec") do
      etcd.elect_listen("lavinmq/leader") do |value|
        # The leader appends its client ports to its advertised URI
        leader1.send nil if value.try &.starts_with?("#{config1.clustering_advertised_uri}?")
      end
    rescue SpecExit
    end
    sleep 0.1.seconds
    spawn(name: "stepdown ctrl1") do
      runner1.run { }
    rescue SpecExit
    end
    leader1.receive # runner1 is leader

    served2 = false
    stepped_down = Channel(Int32).new(1)
    spawn(name: "stepdown ctrl2") do
      runner2.run { served2 = true }
    rescue ex : SpecExit
      stepped_down.send ex.code
    end
    sleep 0.5.seconds # let runner2 queue its election candidacy

    # runner2 falls out of the ISR (e.g. lagging replication) while its
    # candidacy stays queued; the leader then dies.
    etcd.put("lavinmq/isr", runner1.id.to_s(36))
    runner1.shutdown { }
    runner1.close

    # Winning the election out of the ISR means it lacks confirmed data:
    # it must step down instead of serving.
    select
    when code = stepped_down.receive
      code.should eq 3
    when timeout(10.seconds)
      fail "out-of-ISR election winner did not step down"
    end
    served2.should be_false
  ensure
    runner2.try { |r| r.shutdown { } } rescue nil # closes the replication client, its lease is gone
    runner2.try &.close
    FileUtils.rm_rf "/tmp/isr-stepdown1"
    FileUtils.rm_rf "/tmp/isr-stepdown2"
  end

  it "stops without exiting while waiting to be in sync" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      runner = LavinMQ::Clustering::EtcdRunner.new(config)
      LavinMQ::Etcd.new("localhost:12379").put("lavinmq/isr", (runner.id + 1).to_s(36))
      done = Channel(Int32?).new(1)
      spawn(name: "not in sync runner") do
        runner.run { fail "served while not in sync" }
        done.send nil
      rescue ex : SpecExit
        done.send ex.code
      end
      sleep 0.5.seconds # waiting to be in sync
      runner.shutdown { }
      runner.close
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
      end
    end
  end

  # Followers read it once they see a leader, and a watch for it started
  # after the leader wrote it would wait forever
  it "writes the clustering secret before it campaigns" do
    with_datadir do |data_dir|
      etcd = LavinMQ::Etcd.new("localhost:12379")
      # Another node leads, so this one's campaign blocks
      leader_lease = etcd.lease_grant(ttl: 10)
      etcd.election_campaign("lavinmq/leader", "tcp://localhost:5699", leader_lease.id)
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5697"
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      served = Channel(Nil).new(1)
      spawn(name: "campaigning runner") do
        runner.run { served.send nil }
      rescue SpecExit
      end
      begin
        sleep 0.5.seconds # campaigning
        etcd.get("lavinmq/clustering_secret").should_not be_nil
      ensure
        # Lets it win, so that it can be stopped
        leader_lease.release
        select
        when served.receive
        when timeout(5.seconds)
        end
        runner.shutdown { }
        runner.close
      end
    end
  end

  it "stops reporting the replication client's metrics once it leads" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5686"
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      client = LavinMQ::Clustering::Client.new(config, runner.id, "secret", proxy: false)
      runner.following_for_spec(client)
      # follow was never called, satisfy close's follower-done handshake
      spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
      metrics = Channel(String).new(1)
      spawn(name: "promoted runner") do
        runner.run do
          metrics.send(String.build { |io| LavinMQ::HTTP::PrometheusWriter.open(io, "lavinmq") { |w| runner.write_metrics(w) } })
        end
      rescue SpecExit
      end
      begin
        select
        when body = metrics.receive
          body.should_not contain "cluster_received_bytes_total"
        when timeout(5.seconds)
          fail "runner was not elected leader"
        end
      ensure
        runner.shutdown { }
        runner.close
      end
    end
  end

  it "does not reject its own URI while leadership is validating ISR" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://localhost:5685"
      etcd = SelfLeaderEtcd.new(config.clustering_advertised_uri.not_nil!("advertised URI not set"))
      coordinator = LavinMQ::Clustering::EtcdCoordinator.new(config, etcd)
      runner = SelfLeaderRunner.new(config, etcd, coordinator)
      done = Channel(Exception?).new(1)

      spawn(name: "self leader follower monitor spec") do
        runner.follow_leader_public
        done.send nil
      rescue ex
        done.send ex
      end

      etcd.observed.receive
      runner.mark_elected_for_spec

      select
      when ex = done.receive
        ex.should be_nil
      when timeout(500.milliseconds)
        fail "follower monitor kept waiting after this node won the election"
      end
    end
  end

  describe "lease loss" do
    it "exits when its lease expires while following the leader" do
      with_leader_and_candidate("lease-follow", candidate_ttl: 3) do |_leader, candidate, etcd|
        # Like a keepalive that hit "etcdserver: no leader" on 2.10.x: the
        # lease is gone while the node follows as if nothing happened
        etcd.lease_revoke(candidate.id)
        exit_code(candidate, within: 5.seconds).should eq 3
      end
    end

    it "exits once when it steps down for not being in the ISR" do
      with_leader_and_candidate("lease-isr") do |leader, candidate, etcd|
        etcd.put("lavinmq/isr", leader.id.to_s(36))
        leader.shutdown { }
        exit_code(candidate, within: 5.seconds).should eq 3
        # Stepping down releases the lease, which the lease watcher mustn't
        # take for a lost lease
        exit_code(candidate, within: 1.second).should be_nil
        # The stopped leader lost its leadership, the candidate never led
        wait_for { leader.leader_lost_hook_ran? }
        candidate.leader_lost_hook_ran?.should be_false
      end
    end

    it "does not exit when a follower is stopped" do
      with_leader_and_candidate("lease-stop") do |_leader, candidate, _etcd|
        candidate.shutdown { }
        exit_code(candidate, within: 1.second).should be_nil
        candidate.leader_lost_hook_ran?.should be_false
      end
    end

    it "exits when its lease expires while it starts to serve" do
      with_datadir do |data_dir|
        runner = ExitRecordingRunner.new(lease_spec_config(data_dir, 5690, ttl: 3))
        started = Channel(Nil).new
        spawn(name: "lease startup runner") do
          runner.run do
            started.send nil
            sleep # like a startup stuck in flush_isr
          end
        end
        begin
          select
          when started.receive
          when timeout(5.seconds)
            fail "runner was not elected leader"
          end
          LavinMQ::Etcd.new("localhost:12379").lease_revoke(runner.id)
          exit_code(runner, within: 5.seconds).should eq 3
        ensure
          runner.shutdown { } rescue nil
          runner.close
        end
      end
    end
  end

  it "doesn't become leader if it's stopped before it starts" do
    with_datadir do |data_dir|
      # No client ports: #stop doesn't end the follower monitor, so a follower
      # client it makes for a later spec's leader would bind them
      config = lease_spec_config(data_dir, 5687)
      hook_ran = File.join(data_dir, "leader_elected")
      config.clustering_on_leader_elected = "touch #{hook_ran}"
      runner = LavinMQ::Clustering::EtcdRunner.new(config)
      # As a SIGTERM while it waits for the replication client to close
      runner.shutdown { }

      started = Channel(Nil).new
      returned = Channel(Nil).new
      spawn(name: "stopped runner") do
        runner.run { started.close }
        returned.close
      rescue SpecExit
      end

      select
      when returned.receive?
      when started.receive?
        fail "a stopped node became leader"
      when timeout(5.seconds)
        fail "EtcdRunner#run didn't return"
      end

      # The lease is released, so another node can win the election at once
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      lease = etcd.lease_grant(5)
      elected = Channel(Nil).new
      spawn { etcd.election_campaign("lavinmq/leader", "spec", lease.id); elected.close }
      select
      when elected.receive?
      when timeout(1.seconds)
        fail "the leader election wasn't released"
      end
      File.exists?(hook_ran).should be_false
    end
  end

  it "will release lease on shutdown", tags: "slow" do
    with_datadir do |data_dir|
      with_clustered_launcher_config(data_dir) do |config|
        launcher = LavinMQ::Launcher.new(config)

        election_done = Channel(Nil).new
        etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
        spawn do
          etcd.elect_listen("lavinmq/leader") { election_done.close }
        rescue SpecExit
        end

        spawn do
          launcher.run
        rescue SpecExit
        end

        # Wait until our "launcher" is leader
        election_done.receive?

        # The spec gets a lease to use in an election campaign
        lease = etcd.lease_grant(5)
        begin
          # graceful stop...
          spawn { launcher.stop }

          # Let the spec campaign for leadership...
          elected = Channel(Nil).new
          spawn do
            etcd.election_campaign("lavinmq/leader", "spec", lease.id)
            elected.close
          end

          # ... and verify spec is elected
          select
          when elected.receive?
          when timeout(1.seconds)
            fail("election campaign did not finish in time, leadership not released on launcher stop?")
          end
        ensure
          lease.release
        end
      end
    end
  end

  it "creates the data dir and keeps its clustering id across restarts" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = File.join(data_dir, "new")
      id = LavinMQ::Clustering::EtcdRunner.new(config).id
      File.read(File.join(config.data_dir, ".clustering_id")).should eq id.to_s(36)
      File.exists?(File.join(config.data_dir, ".clustering_id.tmp")).should be_false
      LavinMQ::Clustering::EtcdRunner.new(config).id.should eq id
    end
  end

  it "grants its lease with the configured TTL" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5681"
      config.clustering_etcd_lease_ttl = 30
      runner = LavinMQ::Clustering::EtcdRunner.new(config)
      lease_id = File.read(File.join(data_dir, ".clustering_id")).to_i(36)

      elected = Channel(Nil).new
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      spawn do
        etcd.elect_listen("lavinmq/leader") { |value| elected.close if value.try &.starts_with?("#{config.clustering_advertised_uri}?") }
      rescue SpecExit
      end
      spawn do
        runner.run { }
      rescue SpecExit
      end
      begin
        select
        when elected.receive?
        when timeout(5.seconds)
          fail("runner was not elected leader")
        end
        etcd.lease_ttl(lease_id).should be > 10
      ensure
        runner.shutdown { }
        runner.close
      end
    end
  end

  # The replication client shares checksums.sha1 with the replication server,
  # one that never listened has nothing to write to it
  it "keeps the replication checksums when stopped while waiting to be in sync" do
    with_datadir do |data_dir|
      with_clustered_launcher_config(data_dir) do |config|
        launcher = LavinMQ::Launcher.new(config)
        id = File.read(File.join(data_dir, ".clustering_id")).to_i(36)
        LavinMQ::Etcd.new(config.clustering_etcd_endpoints).put("lavinmq/isr", (id + 1).to_s(36))
        checksums_path = File.join(data_dir, "checksums.sha1")
        File.write(checksums_path, "#{Digest::SHA1.digest("a").hexstring} 1 *a\n")
        run_done = Channel(Nil).new
        spawn do
          launcher.run
          run_done.close
        rescue SpecExit
        end
        sleep 0.5.seconds # waiting to be in sync
        launcher.stop
        select
        when run_done.receive?
        when timeout(5.seconds)
          fail("Launcher#run didn't return after stop")
        end
        File.read(checksums_path).should_not be_empty
      end
    end
  end

  it "leaves the exit to the main fiber when stopping a leader", tags: "slow" do
    with_datadir do |data_dir|
      with_clustered_launcher_config(data_dir) do |config|
        launcher = LavinMQ::Launcher.new(config)
        begin
          run_done = Channel(Nil).new
          spawn do
            launcher.run
            run_done.close
          rescue SpecExit
          end
          # The metrics server is pointed at the leader at the end of startup
          wait_for { launcher.@metrics_server.try { |metrics| metrics.@source.@leader.get } }

          # Exiting from the signal handler while #run still closed the replicator
          # closed the log backend under it, and the leader exited with code 1
          launcher.shutdown_for_spec

          select
          when run_done.receive?
          when timeout(1.second)
            fail("Launcher#run didn't return after shutdown")
          end
        ensure
          launcher.stop
        end
      end
    end
  end
end
