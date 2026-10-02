require "../spec_helper"
require "../../src/lavinmq/launcher"
require "../../src/lavinmq/clustering/controller"

private def free_port : Int32
  s = TCPServer.new("127.0.0.1", 0)
  s.local_address.port
ensure
  s.try &.close
end

private def clustering_config(data_dir : String) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering = true
  config.clustering_bind = "127.0.0.1"
  config.clustering_port = free_port
  config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
  config.clustering_raft_port = free_port
  config.clustering_raft_advertised_address = "127.0.0.1:#{config.clustering_raft_port}"
  config.clustering_password = "controller-spec"
  config
end

describe LavinMQ::Clustering::Controller do
  it "uses the etcd backend by default" do
    with_datadir do |data_dir|
      LavinMQ::Clustering::Controller.create(clustering_config(data_dir)).should be_a LavinMQ::Clustering::EtcdController
    end
  end

  it "uses the raft backend when configured" do
    with_datadir do |data_dir|
      config = clustering_config(data_dir)
      config.clustering_backend = LavinMQ::ClusteringBackend::Raft
      LavinMQ::Clustering::Controller.create(config).should be_a LavinMQ::Clustering::RaftController
    end
  end

  it "runs a single node raft cluster from the launcher", tags: "slow" do
    with_datadir do |data_dir|
      config = clustering_config(data_dir)
      config.clustering_backend = LavinMQ::ClusteringBackend::Raft
      config.clustering_election_timeout = 300
      config.clustering_heartbeat_interval = 50
      config.amqp_bind = "127.0.0.1"
      config.amqp_port = free_port
      config.amqps_port = config.https_port = config.mqtts_port = -1
      config.http_port = config.mqtt_port = 0
      config.metrics_http_port = -1
      config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
      config.control_unix_path = File.join(data_dir, "control.sock")
      launcher = LavinMQ::Launcher.new(config)
      stopped = Channel(Nil).new
      spawn(name: "raft launcher spec") do
        launcher.run
      rescue SpecExit
      ensure
        stopped.close
      end
      wait_for(10.seconds) do
        TCPSocket.new("127.0.0.1", config.amqp_port).close
        true
      rescue Socket::ConnectError
        false
      end
      File.exists?(File.join(data_dir, ".raft_state")).should be_true
      launcher.stop
      select
      when stopped.receive?
      when timeout(5.seconds)
        fail "launcher didn't stop"
      end
    end
  end
end
