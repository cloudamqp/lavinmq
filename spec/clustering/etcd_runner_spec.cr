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
  # A follower monitor of the term of *elected*
  def follow_leader_public(elected : BoolChannel)
    follow_leader { follow_leader_changes(elected) }
  end

  def repli_client : LavinMQ::Clustering::Client?
    @repli_client.get
  end
end

# Observes the leaders a spec sends
private class ScriptedEtcd < LavinMQ::Etcd
  getter leaders = Channel(String).new

  def initialize
    super("localhost:1")
  end

  def elect_listen(_name, &)
    while uri = @leaders.receive?
      yield uri
    end
  end

  def get(_key) : String?
    "secret"
  end
end

# The clustering secret is only there once a spec sends it
private class SecretlessEtcd < ScriptedEtcd
  getter watching = Channel(Nil).new(1)
  getter secret = Channel(String).new

  def get(_key) : String?
  end

  def watch(_key, &)
    @watching.send nil
    yield @secret.receive
  end
end

# No follower monitor: a replication client set with #following_for_spec is
# left to #run, as when this node is elected before the monitor sees it
# Revoking a lease hangs, as while etcd is unavailable, until *gate* is closed
private class HangingRevokeEtcd < LavinMQ::Etcd
  getter gate = Channel(Nil).new

  def lease_revoke(id) : Nil
    @gate.receive?
    super
  end
end

private class UnmonitoredRunner < LavinMQ::Clustering::EtcdRunner
  def following_for_spec(client : LavinMQ::Clustering::Client) : Nil
    @repli_client.set(client)
  end

  def lease_for_spec=(lease : LavinMQ::Etcd::Lease)
    @lease.set(lease)
  end

  private def follow_leader_changes(elected)
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

# Rejects ISR writes once *stale* is set, as etcd does once this node's
# election key is gone
private class StaleOnDemandCoordinator < LavinMQ::Clustering::EtcdCoordinator
  property? stale = false

  def update_isr(synced_node_ids : Set(Int32)) : Nil
    raise StaleLeadership.new("Not the leader (spec)") if stale?
    super
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
  # The SIGTERM handler would never get to exit
  it "stops when releasing its lease fails" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      etcd = FailingRevokeEtcd.new
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      runner.lease_for_spec = LavinMQ::Etcd::Lease.grant(etcd, 1i64, 10)
      runner.stop
      runner.broker_closed
    end
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
    config1.amqp_port = 5671
    config1.http_port = 15671
    runner1 = LavinMQ::Clustering::EtcdRunner.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/failover2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5682"
    config2.clustering_port = 5682
    config2.amqp_port = 5672
    config2.http_port = 15672
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
    when /1$/
      runner1.stop
      runner1.broker_closed
      listen.receive.should match /2$/
      sleep 0.1.seconds
      runner2.stop
      runner2.broker_closed
    when /2$/
      runner2.stop
      runner2.broker_closed
      listen.receive.should match /1$/
      sleep 0.1.seconds
      runner1.stop
      runner1.broker_closed
    else fail("no leader elected")
    end
  ensure
    runner1.try &.stop rescue nil
    runner1.try &.broker_closed
    runner2.try &.stop rescue nil
    runner2.try &.broker_closed
  end

  it "steps down if it wins the election while not in the ISR" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/isr-stepdown1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5683"
    FileUtils.rm_rf config1.data_dir
    runner1 = LavinMQ::Clustering::EtcdRunner.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/isr-stepdown2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5684"
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
    runner1.stop
    runner1.broker_closed

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
    runner2.try &.stop rescue nil # closes the replication client, its lease is gone
    runner2.try &.broker_closed
    FileUtils.rm_rf "/tmp/isr-stepdown1"
    FileUtils.rm_rf "/tmp/isr-stepdown2"
  end

  it "stops serving when its lease expires, and leads again without exiting" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5686"
      config.clustering_etcd_lease_ttl = 3 # keepalive every second
      runner = LavinMQ::Clustering::EtcdRunner.new(config)
      demoted = Channel(Proc(Nil)?).new(1)
      fenced_at_demote = nil
      runner.on_demote do |hand_over|
        # Nothing more confirmed while the Launcher closes the broker
        fenced_at_demote = runner.@replicator.get.try &.fenced?
        runner.broker_closed # as the Launcher does once it closed the broker
        demoted.send hand_over
      end
      served = Channel(Nil).new(2)
      done = Channel(Int32?).new(1)
      spawn(name: "lease expiry runner") do
        runner.run { served.send nil }
        done.send nil
      rescue ex : SpecExit
        done.send ex.code
      end
      select
      when served.receive
      when timeout(10.seconds)
        fail "wasn't elected"
      end
      LavinMQ::Etcd.new("localhost:12379").lease_revoke(runner.id)
      select
      when hand_over = demoted.receive
        hand_over.should be_nil # lost, not handed over
        fenced_at_demote.should be_true
      when code = done.receive
        fail "exited with #{code.inspect} instead of stopping to serve"
      when timeout(10.seconds)
        fail "didn't stop serving when its lease expired"
      end
      select
      when served.receive
      when code = done.receive
        fail "exited with #{code.inspect} instead of leading again"
      when timeout(10.seconds)
        fail "wasn't elected again"
      end
      runner.stop
      runner.broker_closed
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
      end
    end
  end

  # Nothing can be confirmed while the coordinator rejects its ISR writes,
  # e.g. after its election key was deleted, so it hands over
  it "stops serving when its replication is fenced while its lease lives" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5698"
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      coordinator = StaleOnDemandCoordinator.new(config, etcd)
      runner = UnmonitoredRunner.new(config, etcd, coordinator)
      demoted = Channel(Nil).new(1)
      runner.on_demote do
        runner.broker_closed
        demoted.send nil
      end
      served = Channel(LavinMQ::Clustering::Server).new(1)
      spawn(name: "fenced runner") do
        runner.run { |replicator| served.send replicator.as(LavinMQ::Clustering::Server) }
      rescue SpecExit
      end
      replicator = select
      when r = served.receive
        r
      when timeout(10.seconds)
        fail "wasn't elected"
      end
      coordinator.stale = true
      expect_raises(LavinMQ::Clustering::Coordinator::StaleLeadership) { replicator.flush_isr }
      select
      when demoted.receive
      when timeout(10.seconds)
        fail "kept serving with fenced replication"
      end
    ensure
      runner.try &.stop
      runner.try &.broker_closed
    end
  end

  # Revoking the lost lease retries while etcd is unavailable, which is when
  # leases are lost, and a node that isn't the leader must not keep serving
  it "stops serving when its lease expires, also while the lease can't be revoked" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5696"
      config.clustering_etcd_lease_ttl = 3 # keepalive every second
      etcd = HangingRevokeEtcd.new("localhost:12379")
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      demoted = Channel(Nil).new(1)
      runner.on_demote do
        runner.broker_closed
        demoted.send nil
      end
      served = Channel(Nil).new(2)
      spawn(name: "hanging revoke runner") do
        runner.run { served.send nil }
      rescue SpecExit
      end
      select
      when served.receive
      when timeout(10.seconds)
        fail "wasn't elected"
      end
      LavinMQ::Etcd.new("localhost:12379").lease_revoke(runner.id)
      select
      when demoted.receive
      when timeout(10.seconds)
        fail "kept serving while revoking the lost lease"
      end
    ensure
      etcd.try &.gate.close
      runner.try &.stop rescue nil # the lease is gone
    end
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
      runner.stop
      runner.broker_closed
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
      end
    end
  end

  it "stops without exiting while campaigning to lead" do
    with_datadir do |data_dir|
      # Another node leads, so this one's campaign blocks
      etcd = LavinMQ::Etcd.new("localhost:12379")
      leader_lease = etcd.lease_grant(ttl: 10)
      etcd.election_campaign("lavinmq/leader", "tcp://localhost:5699", leader_lease.id)
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5698"
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      done = Channel(Int32?).new(1)
      spawn(name: "campaigning runner") do
        runner.run { fail "served while another node leads" }
        done.send nil
      rescue ex : SpecExit
        done.send ex.code
      end
      sleep 0.5.seconds # campaigning
      runner.stop
      runner.broker_closed
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
      end
    ensure
      leader_lease.try &.release
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
        runner.stop
        runner.broker_closed
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
      client = LavinMQ::Clustering::Client.new(config, runner.id, "secret")
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
        runner.stop
        runner.broker_closed
      end
    end
  end

  # Client connections then wait for this node to serve, see ClientListeners
  it "tells that there's no leader host before it serves" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5687"
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      runner = UnmonitoredRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      client = LavinMQ::Clustering::Client.new(config, runner.id, "secret")
      runner.following_for_spec(client)
      # follow was never called, satisfy close's follower-done handshake
      spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
      changed = Atomic(Bool).new(false)
      runner.on_leader_change { changed.set(true) }
      served = Channel(Bool).new(1)
      spawn(name: "promoted runner") do
        runner.run { served.send changed.get }
      rescue SpecExit
      end
      begin
        select
        when told = served.receive
          told.should be_true
        when timeout(5.seconds)
          fail "runner was not elected leader"
        end
      ensure
        runner.stop
        runner.broker_closed
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
      elected = BoolChannel.new(false)

      spawn(name: "self leader follower monitor spec") do
        runner.follow_leader_public(elected)
        done.send nil
      rescue ex
        done.send ex
      end

      etcd.observed.receive
      elected.set(true)

      select
      when ex = done.receive
        ex.should be_nil
      when timeout(500.milliseconds)
        fail "follower monitor kept waiting after this node won the election"
      end
    end
  end

  # A short term can end before its monitor sees this node's own URI, the
  # next term has a monitor of its own
  it "doesn't follow from a follower monitor of a term that has ended" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      own_uri = config.clustering_advertised_uri = "tcp://localhost:5688"
      etcd = ScriptedEtcd.new
      runner = SelfLeaderRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      elected = BoolChannel.new(false)
      elected.set(true) # this node was elected in that term
      [own_uri, "tcp://127.0.0.1:1"].each do |leader|
        done = Channel(Exception?).new(1)
        spawn(name: "follower monitor of an ended term") do
          runner.follow_leader_public(elected)
          done.send nil
        rescue ex
          done.send ex
        end
        etcd.leaders.send leader
        select
        when ex = done.receive
          ex.should be_nil
        when timeout(5.seconds)
          fail "follower monitor of an ended term kept following"
        end
        runner.repli_client.should be_nil
      end
    end
  end

  # A replication client takes over checksums.sha1 when it's made
  it "makes no replication client in a term that ended while it waited for the secret" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://localhost:5689"
      checksums = File.join(data_dir, "checksums.sha1")
      File.write(checksums, "#{Digest::SHA1.digest("a").hexstring} 1 *a\n")
      etcd = SecretlessEtcd.new
      runner = SelfLeaderRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      elected = BoolChannel.new(false)
      done = Channel(Exception?).new(1)
      spawn(name: "follower monitor waiting for the secret") do
        runner.follow_leader_public(elected)
        done.send nil
      rescue ex
        done.send ex
      end
      etcd.leaders.send "tcp://127.0.0.1:1"
      etcd.watching.receive
      elected.set(true) # this node was elected meanwhile
      etcd.secret.send "secret"
      select
      when ex = done.receive
        ex.should be_nil
      when timeout(5.seconds)
        fail "follower monitor didn't return"
      end
      runner.repli_client.should be_nil
      File.read(checksums).should_not be_empty
    end
  end

  it "makes no replication client once stopped while it waited for the secret" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://localhost:5689"
      etcd = SecretlessEtcd.new
      runner = SelfLeaderRunner.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      done = Channel(Exception?).new(1)
      spawn(name: "follower monitor waiting for the secret") do
        runner.follow_leader_public(BoolChannel.new(false))
        done.send nil
      rescue ex
        done.send ex
      end
      etcd.leaders.send "tcp://127.0.0.1:1"
      etcd.watching.receive
      runner.stop
      runner.broker_closed
      etcd.secret.send "secret"
      select
      when ex = done.receive
        ex.should be_nil
      when timeout(5.seconds)
        fail "follower monitor didn't return"
      end
      runner.repli_client.should be_nil
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
        runner.stop
        runner.broker_closed
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
