require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/launcher"
require "../../src/lavinmq/clustering/controller"

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

private class SelfLeaderController < LavinMQ::Clustering::EtcdController
  def follow_leader_public
    follow_leader
  end

  def mark_elected_for_spec
    @elected_leader.set(true)
  end
end

# No follower monitor: a replication client set with #following_for_spec is
# left to #run, as when this node is elected before the monitor sees it
private class UnmonitoredController < LavinMQ::Clustering::EtcdController
  def following_for_spec(client : LavinMQ::Clustering::Client) : Nil
    @repli_client.set(client)
  end

  private def follow_leader_changes
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

describe LavinMQ::Clustering::EtcdController, tags: "etcd" do
  add_etcd_around_each

  it "will failover", tags: "slow" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/failover1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5681"
    config1.clustering_port = 5681
    config1.amqp_port = 5671
    config1.http_port = 15671
    controller1 = LavinMQ::Clustering::EtcdController.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/failover2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5682"
    config2.clustering_port = 5682
    config2.amqp_port = 5672
    config2.http_port = 15672
    controller2 = LavinMQ::Clustering::EtcdController.new(config2)

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
      controller1.run { }
    rescue SpecExit
    end
    spawn(name: "failover2") do
      controller2.run { }
    rescue SpecExit
    end
    sleep 0.1.seconds
    leader = listen.receive
    case leader
    when /1$/
      controller1.stop
      listen.receive.should match /2$/
      sleep 0.1.seconds
      controller2.stop
    when /2$/
      controller2.stop
      listen.receive.should match /1$/
      sleep 0.1.seconds
      controller1.stop
    else fail("no leader elected")
    end
  end

  it "steps down if it wins the election while not in the ISR" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/isr-stepdown1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5683"
    FileUtils.rm_rf config1.data_dir
    controller1 = LavinMQ::Clustering::EtcdController.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/isr-stepdown2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5684"
    FileUtils.rm_rf config2.data_dir
    controller2 = LavinMQ::Clustering::EtcdController.new(config2)

    etcd = LavinMQ::Etcd.new("localhost:12379")
    leader1 = Channel(Nil).new
    spawn(name: "elect listen spec") do
      etcd.elect_listen("lavinmq/leader") do |value|
        leader1.send nil if value == config1.clustering_advertised_uri
      end
    rescue SpecExit
    end
    sleep 0.1.seconds
    spawn(name: "stepdown ctrl1") do
      controller1.run { }
    rescue SpecExit
    end
    leader1.receive # controller1 is leader

    served2 = false
    stepped_down = Channel(Int32).new(1)
    spawn(name: "stepdown ctrl2") do
      controller2.run { served2 = true }
    rescue ex : SpecExit
      stepped_down.send ex.code
    end
    sleep 0.5.seconds # let controller2 queue its election candidacy

    # controller2 falls out of the ISR (e.g. lagging replication) while its
    # candidacy stays queued; the leader then dies.
    etcd.put("lavinmq/isr", controller1.id.to_s(36))
    controller1.stop

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
      controller = LavinMQ::Clustering::EtcdController.new(config)
      demoted = Channel(Proc(Nil)?).new(1)
      controller.on_demote { |hand_over| demoted.send hand_over }
      served = Channel(Nil).new(2)
      done = Channel(Int32?).new(1)
      spawn(name: "lease expiry controller") do
        controller.run { served.send nil }
        done.send nil
      rescue ex : SpecExit
        done.send ex.code
      end
      select
      when served.receive
      when timeout(10.seconds)
        fail "wasn't elected"
      end
      LavinMQ::Etcd.new("localhost:12379").lease_revoke(controller.id)
      select
      when hand_over = demoted.receive
        hand_over.should be_nil # lost, not handed over
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
      controller.stop
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
      end
    end
  end

  it "stops without exiting while waiting to be in sync" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5685"
      controller = LavinMQ::Clustering::EtcdController.new(config)
      LavinMQ::Etcd.new("localhost:12379").put("lavinmq/isr", (controller.id + 1).to_s(36))
      done = Channel(Int32?).new(1)
      spawn(name: "not in sync controller") do
        controller.run { fail "served while not in sync" }
        done.send nil
      rescue ex : SpecExit
        done.send ex.code
      end
      sleep 0.5.seconds # waiting to be in sync
      controller.stop
      select
      when code = done.receive
        code.should be_nil
      when timeout(5.seconds)
        fail "run didn't return after stop"
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
      controller = UnmonitoredController.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      client = LavinMQ::Clustering::Client.new(config, controller.id, "secret")
      controller.following_for_spec(client)
      # follow was never called, satisfy close's follower-done handshake
      spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
      metrics = Channel(String).new(1)
      spawn(name: "promoted controller") do
        controller.run do
          metrics.send(String.build { |io| LavinMQ::HTTP::PrometheusWriter.open(io, "lavinmq") { |w| controller.write_metrics(w) } })
        end
      rescue SpecExit
      end
      begin
        select
        when body = metrics.receive
          body.should_not contain "cluster_received_bytes_total"
        when timeout(5.seconds)
          fail "controller was not elected leader"
        end
      ensure
        controller.stop
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
      controller = UnmonitoredController.new(config, etcd, LavinMQ::Clustering::EtcdCoordinator.new(config, etcd))
      client = LavinMQ::Clustering::Client.new(config, controller.id, "secret")
      controller.following_for_spec(client)
      # follow was never called, satisfy close's follower-done handshake
      spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
      changed = Atomic(Bool).new(false)
      controller.on_leader_host_change { changed.set(true) }
      served = Channel(Bool).new(1)
      spawn(name: "promoted controller") do
        controller.run { served.send changed.get }
      rescue SpecExit
      end
      begin
        select
        when told = served.receive
          told.should be_true
        when timeout(5.seconds)
          fail "controller was not elected leader"
        end
      ensure
        controller.stop
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
      controller = SelfLeaderController.new(config, etcd, coordinator)
      done = Channel(Exception?).new(1)

      spawn(name: "self leader follower monitor spec") do
        controller.follow_leader_public
        done.send nil
      rescue ex
        done.send ex
      end

      etcd.observed.receive
      controller.mark_elected_for_spec

      select
      when ex = done.receive
        ex.should be_nil
      when timeout(500.milliseconds)
        fail "follower monitor kept waiting after this node won the election"
      end
    end
  end

  it "will release lease on shutdown", tags: "slow" do
    config = LavinMQ::Config.new
    config.data_dir = "/tmp/release-lease"
    config.clustering = true
    config.clustering_etcd_endpoints = "localhost:12379"
    config.clustering_advertised_uri = "tcp://localhost:5681"
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
    lease.try &.release
  end

  it "creates the data dir and keeps its clustering id across restarts" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = File.join(data_dir, "new")
      id = LavinMQ::Clustering::EtcdController.new(config).id
      File.read(File.join(config.data_dir, ".clustering_id")).should eq id.to_s(36)
      File.exists?(File.join(config.data_dir, ".clustering_id.tmp")).should be_false
      LavinMQ::Clustering::EtcdController.new(config).id.should eq id
    end
  end

  it "grants its lease with the configured TTL" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      config.clustering_advertised_uri = "tcp://localhost:5681"
      config.clustering_etcd_lease_ttl = 30
      controller = LavinMQ::Clustering::EtcdController.new(config)
      lease_id = File.read(File.join(data_dir, ".clustering_id")).to_i(36)

      elected = Channel(Nil).new
      etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
      spawn do
        etcd.elect_listen("lavinmq/leader") { |value| elected.close if value == config.clustering_advertised_uri }
      rescue SpecExit
      end
      spawn do
        controller.run { }
      rescue SpecExit
      end
      begin
        select
        when elected.receive?
        when timeout(5.seconds)
          fail("controller was not elected leader")
        end
        etcd.lease_ttl(lease_id).should be > 10
      ensure
        controller.stop
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
          wait_for { launcher.@server }

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
