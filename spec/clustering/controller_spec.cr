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
  config.clustering_secret = "controller-spec"
  config
end

# A raft cluster of real launchers in this process, one per data dir, each
# voter needed for a quorum when there are two
private def raft_launcher_configs(dirs : Enumerable(String)) : Array(LavinMQ::Config)
  configs = dirs.map do |dir|
    config = clustering_config(dir)
    config.clustering_backend = LavinMQ::ClusteringBackend::Raft
    config.clustering_election_timeout = 300
    config.clustering_heartbeat_interval = 50
    config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
    config.amqp_port = config.http_port = config.mqtt_port = 0
    config.amqps_port = config.https_port = config.mqtts_port = -1
    config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
    config.control_unix_path = File.join(dir, "control.sock")
    config.metrics_http_bind = "127.0.0.1"
    config.metrics_http_port = free_port
    config
  end.to_a
  seeds = configs.join(',', &.clustering_raft_advertised_address)
  configs.each &.clustering_seeds = seeds
  configs[0].clustering_bootstrap = true
  configs
end

private def run_launcher(launcher : LavinMQ::Launcher, exited : Channel(Nil)) : Nil
  spawn(name: "raft launcher spec") do
    launcher.run
  rescue ex : SpecExit
    STDERR.puts "launcher exited with #{ex.code}"
  ensure
    exited.send nil
  end
end

private def stop_launchers(launchers, exited) : Nil
  launchers.reverse_each &.stop
  launchers.size.times do
    select
    when exited.receive
    when timeout(10.seconds) then break
    end
  end
end

private def metrics_of(config : LavinMQ::Config) : String
  HTTP::Client.get("http://127.0.0.1:#{config.metrics_http_port}/metrics").body
rescue
  ""
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

  it "follows a leader while the launcher holds the data dir lock", tags: "slow" do
    with_datadir do |leader_dir|
      with_datadir do |follower_dir|
        configs = {leader_dir, follower_dir}.map do |dir|
          config = clustering_config(dir)
          config.clustering_backend = LavinMQ::ClusteringBackend::Raft
          config.clustering_election_timeout = 300
          config.clustering_heartbeat_interval = 50
          config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
          config.amqp_port = config.http_port = config.mqtt_port = 0
          config.amqps_port = config.https_port = config.mqtts_port = -1
          config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
          config.control_unix_path = File.join(dir, "control.sock")
          config.metrics_http_bind = "127.0.0.1"
          config.metrics_http_port = free_port
          config.data_dir_lock = true
          config
        end
        seeds = configs.join(',', &.clustering_raft_advertised_address)
        configs.each &.clustering_seeds = seeds
        configs[0].clustering_bootstrap = true
        launchers = configs.map { |c| LavinMQ::Launcher.new(c) }
        launchers.each do |l|
          spawn(name: "raft launcher spec") do
            l.run
          rescue SpecExit
          end
        end
        # The replication client would wait forever for the lock its own
        # launcher holds, and never report
        metrics = "http://127.0.0.1:#{configs[1].metrics_http_port}/metrics"
        wait_for(10.seconds) do
          body = HTTP::Client.get(metrics).body rescue ""
          body.includes?("lavinmq_cluster_received_bytes_total") && body.includes?("lavinmq_raft_has_leader 1")
        end
      ensure
        launchers.try &.reverse_each &.stop
      end
    end
  end

  # Two voters, so each needs the other for a quorum: a leader that exited to
  # hand over would leave the new leader without one until it restarted.
  it "hands leadership back and forth without restarting the process", tags: "slow" do
    with_datadir do |dir_a|
      with_datadir do |dir_b|
        configs = raft_launcher_configs({dir_a, dir_b})
        launchers = configs.map { |c| LavinMQ::Launcher.new(c) }
        exited = Channel(Nil).new(2)
        launchers.each { |l| run_launcher(l, exited) }
        controllers = launchers.map { |l| l.@raft_controller.not_nil! }
        serving = ->(i : Int32) { metrics_of(configs[i]).includes?("lavinmq_uptime") }
        following = ->(i : Int32) { metrics_of(configs[i]).includes?("lavinmq_cluster_received_bytes_total") }
        in_sync = ->(i : Int32) { controllers[1 - i].node.committed_isr.try(&.includes?(controllers[i].id)) || false }
        state = -> do
          {0, 1}.map do |i|
            node = controllers[i].node
            "node #{i}: leader #{node.leader?}, serving #{serving.call(i)}, following #{following.call(i)}, " \
            "leader_uri #{node.leader_uri.inspect}, isr #{node.committed_isr.try(&.to_a).inspect}"
          end.join("; ")
        end

        wait_for(10.seconds) { serving.call(0) && following.call(1) && in_sync.call(1) }
        {0, 1}.each do |from|
          to = 1 - from
          plan = controllers[from].request_transfer.as(LavinMQ::Clustering::RaftController::Transfer)
          plan.target.should eq controllers[to].id
          controllers[from].step_down(plan)
          # The old leader follows the new one, in the same process
          begin
            wait_for(20.seconds) { serving.call(to) && following.call(from) && in_sync.call(from) }
          rescue ex
            fail "handover from node #{from} to #{to} didn't complete (#{ex.message}): #{state.call}"
          end
          serving.call(from).should be_false
          # and still counts for its quorum, so the new leader keeps leading
          sleep 1.second # over three election timeouts
          controllers[to].node.leader?.should be_true
          serving.call(to).should be_true
        end
        select
        when exited.receive
          fail "a launcher returned while it should keep running"
        else
        end
      ensure
        stop_launchers(launchers, exited) if launchers && exited
      end
    end
  end

  it "stops serving on losing its quorum and leads again, without restarting the process", tags: "slow" do
    with_datadir do |dir_a|
      with_datadir do |dir_b|
        configs = raft_launcher_configs({dir_a, dir_b})
        launchers = configs.map { |c| LavinMQ::Launcher.new(c) }
        exited = Channel(Nil).new(3)
        launchers.each { |l| run_launcher(l, exited) }
        leader = launchers[0].@raft_controller.not_nil!
        wait_for(10.seconds) do
          metrics_of(configs[0]).includes?("lavinmq_uptime") &&
            leader.node.committed_isr.try(&.includes?(launchers[1].@raft_controller.not_nil!.id))
        end

        # The other voter goes away, the leader can't keep its quorum
        launchers[1].stop
        exited.receive
        wait_for(5.seconds) { !leader.node.leader? }
        wait_for(5.seconds) { !metrics_of(configs[0]).includes?("lavinmq_uptime") }
        metrics_of(configs[0]).should contain "lavinmq_raft_has_leader 0"

        # It comes back, and a leader serves again
        launchers[1] = LavinMQ::Launcher.new(configs[1])
        run_launcher(launchers[1], exited)
        wait_for(10.seconds) { configs.any? { |c| metrics_of(c).includes?("lavinmq_uptime") } }
        select
        when exited.receive
          fail "a launcher returned while it should keep running"
        else
        end
      ensure
        stop_launchers(launchers, exited) if launchers && exited
      end
    end
  end
end
