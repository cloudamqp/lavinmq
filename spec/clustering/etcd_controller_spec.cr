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

describe LavinMQ::Clustering::EtcdController do
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
      controller = SelfLeaderController.new(config, etcd, coordinator)

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

  it "does not reject its own URI while leadership is validating ISR" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.clustering_advertised_uri = "tcp://localhost:5685"
      etcd = SelfLeaderEtcd.new(config.clustering_advertised_uri.not_nil!)
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
  end
end
