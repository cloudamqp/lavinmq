require "../spec_helper"
require "../../src/lavinmq/clustering_v2/raft_node"

module ClusteringV2ConsensusSpec
  class TestCluster
    getter nodes = Array(LavinMQ::ClusteringV2::RaftNode).new
    getter dirs = Array(String).new
    getter ports = Array(Int32).new
    getter secret = "shared_cluster_secret_key_for_v2_tests"

    def initialize(count : Int32, index_capacity : Int32 = 128)
      @ports = Array(Int32).new(count) do
        srv = TCPServer.new("127.0.0.1", 0)
        port = srv.local_address.port
        srv.close
        port
      end

      peers = @ports.map_with_index do |port, idx|
        LavinMQ::ClusteringV2::MemberInfo.new(
          (idx + 1).to_u64,
          "tcp://127.0.0.1:#{port}",
          "amqp://127.0.0.1:567#{idx + 1}",
          "http://127.0.0.1:1567#{idx + 1}",
          LavinMQ::ClusteringV2::MemberRole::Voter
        )
      end

      count.times do |idx|
        node_id = (idx + 1).to_u64
        dir = File.tempname("lavinmq_raft_node_#{node_id}_")
        Dir.mkdir_p(dir)
        @dirs << dir
        log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: index_capacity.to_u64, node_id: node_id)
        node = LavinMQ::ClusteringV2::RaftNode.new(
          log: log,
          secret: @secret,
          peer_uri: "tcp://127.0.0.1:#{@ports[idx]}",
          amqp_uri: "amqp://127.0.0.1:567#{node_id}",
          http_uri: "http://127.0.0.1:1567#{node_id}",
          initial_role: LavinMQ::ClusteringV2::MemberRole::Voter
        )
        node.heartbeat_interval = 20.milliseconds
        node.election_timeout_min = 80.milliseconds
        node.election_timeout_max = 150.milliseconds
        node.leader_lease_timeout = 180.milliseconds
        node.leader_stickiness_window = 60.milliseconds
        node.learner_promote_threshold = 4_u64
        node.configure_cluster(peers)
        @nodes << node
      end
    end

    def start_all
      @nodes.each_with_index do |node, idx|
        node.start("127.0.0.1", @ports[idx])
      end
    end

    def wait_for_leader(timeout : Time::Span = 5.seconds) : LavinMQ::ClusteringV2::RaftNode
      deadline = Time.instant + timeout
      while Time.instant < deadline
        leaders = @nodes.select(&.leader?)
        if leaders.size == 1
          return leaders.first
        end
        sleep 10.milliseconds
      end
      raise "Timed out waiting for a single leader (roles: #{@nodes.map { |n| "#{n.node_id}:#{n.role}" }})"
    end

    def cleanup
      @nodes.each(&.close)
      @dirs.each { |d| FileUtils.rm_rf(d) }
    end
  end
end

describe LavinMQ::ClusteringV2::RaftNode do
  it "elects a single leader in a 3-node cluster and replicates majority-quorum entries" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3)
    begin
      cluster.nodes.each do |node|
        node.execution_context.should_not eq(Fiber::ExecutionContext.default)
      end

      cluster.start_all
      leader = cluster.wait_for_leader

      applied = Channel({UInt64, Bytes, Fiber::ExecutionContext}).new(30)
      cluster.nodes.each do |node|
        node.on_apply do |rec|
          if rec.entry.cmd_type == LavinMQ::ClusteringV2::LogCommandType::QueueDeclare.value
            applied.send({node.node_id, rec.payload.dup, Fiber::ExecutionContext.current})
          end
        end
      end

      entry = leader.propose_command(
        LavinMQ::ClusteringV2::LogCommandType::QueueDeclare,
        "orders.created".to_slice
      )
      leader.wait_for_commit(entry.lsn, 2.seconds).should be_true

      received = Set(UInt64).new
      3.times do
        select
        when msg = applied.receive
          node_id, payload, ctx = msg
          payload.should eq("orders.created".to_slice)
          node = cluster.nodes.find! { |n| n.node_id == node_id }
          ctx.should eq(node.execution_context)
          ctx.should_not eq(Fiber::ExecutionContext.default)
          received.add(node_id)
        when timeout(2.seconds)
          fail "Timed out waiting for nodes to apply entry"
        end
      end
      received.should eq(Set{1_u64, 2_u64, 3_u64})
    ensure
      cluster.cleanup
    end
  end

  it "continues committing under strict majority quorum when 1 of 3 followers is slow/paused" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3)
    begin
      cluster.start_all
      leader = cluster.wait_for_leader
      followers = cluster.nodes.reject { |n| n.node_id == leader.node_id }
      slow_follower = followers.first
      healthy_follower = followers.last

      # Pause traffic between leader and slow_follower
      leader.pause_peer(slow_follower.node_id, true)
      slow_follower.pause_peer(leader.node_id, true)

      entry = leader.propose_command(
        LavinMQ::ClusteringV2::LogCommandType::ExchangeDeclare,
        "events.topic".to_slice
      )

      # Strict majority (leader + healthy_follower = 2/3) commits immediately without stalling on slow_follower
      leader.wait_for_commit(entry.lsn, 1.second).should be_true

      wait_for(2.seconds) { healthy_follower.commit_index >= entry.lsn }
      healthy_follower.commit_index.should be >= entry.lsn
      slow_follower.commit_index.should be < entry.lsn

      # Unpause slow follower and verify it catches up automatically
      leader.pause_peer(slow_follower.node_id, false)
      slow_follower.pause_peer(leader.node_id, false)
      wait_for(2.seconds) { slow_follower.commit_index >= entry.lsn }
      slow_follower.commit_index.should be >= entry.lsn
    ensure
      cluster.cleanup
    end
  end

  it "steps down cleanly in-process when isolated from majority quorum and elects a new leader" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3)
    begin
      cluster.start_all
      old_leader = cluster.wait_for_leader
      followers = cluster.nodes.reject { |n| n.node_id == old_leader.node_id }

      stepped_down = Channel(UInt64).new(4)
      old_leader.on_step_down do |term|
        stepped_down.send(term)
      end

      # Isolate old_leader from both followers
      old_leader.set_isolated(true)
      followers.each do |f|
        f.pause_peer(old_leader.node_id, true)
      end

      # Old leader must step down in-process after lease expiry (no exit 3!)
      select
      when _term = stepped_down.receive
      when timeout(2.seconds)
        fail "Isolated leader did not step down within lease timeout"
      end
      old_leader.role.should eq(LavinMQ::ClusteringV2::Role::Follower)

      # Remaining 2 followers form a majority and elect a new leader
      deadline = Time.instant + 3.seconds
      new_leader = nil
      while Time.instant < deadline
        if l = followers.find(&.leader?)
          new_leader = l
          break
        end
        sleep 10.milliseconds
      end
      new_leader.should_not be_nil
      new_leader.not_nil!.node_id.should_not eq(old_leader.node_id)
      new_leader.not_nil!.current_term.should be > old_leader.current_term
    ensure
      cluster.cleanup
    end
  end

  it "onboards a 4th node as a non-voting Learner and auto-promotes it to Voter once caught up" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3)
    learner_dir = File.tempname("lavinmq_raft_learner_")
    Dir.mkdir_p(learner_dir)
    learner_node = nil
    begin
      cluster.start_all
      leader = cluster.wait_for_leader

      # Write entries before the 4th node exists
      10.times do |i|
        e = leader.propose_command(
          LavinMQ::ClusteringV2::LogCommandType::QueueDeclare,
          "q.#{i}".to_slice
        )
        leader.wait_for_commit(e.lsn, 1.second).should be_true
      end

      srv = TCPServer.new("127.0.0.1", 0)
      learner_port = srv.local_address.port
      srv.close

      learner_info = LavinMQ::ClusteringV2::MemberInfo.new(
        4_u64,
        "tcp://127.0.0.1:#{learner_port}",
        "amqp://127.0.0.1:5674",
        "http://127.0.0.1:15674",
        LavinMQ::ClusteringV2::MemberRole::Learner
      )

      seed_peers = cluster.nodes.map do |n|
        LavinMQ::ClusteringV2::MemberInfo.new(
          n.node_id, n.peer_uri, n.amqp_uri, n.http_uri,
          LavinMQ::ClusteringV2::MemberRole::Voter
        )
      end
      seed_peers << learner_info

      learner_log = LavinMQ::ClusteringV2::RaftLog.new(learner_dir, capacity: 128_u64, node_id: 4_u64)
      learner_node = LavinMQ::ClusteringV2::RaftNode.new(
        log: learner_log,
        secret: cluster.secret,
        peer_uri: learner_info.peer_uri,
        amqp_uri: learner_info.amqp_uri,
        http_uri: learner_info.http_uri,
        initial_role: LavinMQ::ClusteringV2::MemberRole::Learner
      )
      learner_node.heartbeat_interval = 20.milliseconds
      learner_node.election_timeout_min = 80.milliseconds
      learner_node.election_timeout_max = 150.milliseconds
      learner_node.configure_cluster(seed_peers)
      learner_node.start("127.0.0.1", learner_port)

      # Add learner to leader; leader will stream log and auto-promote to Voter
      conf_entry = leader.propose_conf_change(
        LavinMQ::ClusteringV2::ConfChangeType::AddLearner,
        learner_info
      )
      leader.wait_for_commit(conf_entry.lsn, 2.seconds).should be_true

      wait_for(3.seconds) do
        leader.members.find { |m| m.node_id == 4_u64 }.try(&.voter?) == true &&
          learner_node.not_nil!.commit_index >= conf_entry.lsn &&
          learner_node.not_nil!.role.follower?
      end

      leader.members.find! { |m| m.node_id == 4_u64 }.role.should eq(LavinMQ::ClusteringV2::MemberRole::Voter)
      learner_node.not_nil!.role.should eq(LavinMQ::ClusteringV2::Role::Follower)
    ensure
      learner_node.try(&.close)
      FileUtils.rm_rf(learner_dir)
      cluster.cleanup
    end
  end

  it "falls back to InstallSnapshot when a follower lags behind compacted raft.idx" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3, index_capacity: 32)
    begin
      cluster.start_all
      leader = cluster.wait_for_leader
      followers = cluster.nodes.reject { |n| n.node_id == leader.node_id }
      lagging_follower = followers.first

      snapshot_installed = Channel(Bytes).new(2)
      leader.on_build_snapshot do |_lsn|
        [LavinMQ::ClusteringV2::SnapshotSegmentFile.new("/", "q1", 1_u32, "vhost1/q1/msgs.0000000001", "snapshot-state-v1".to_slice)]
      end
      lagging_follower.on_install_snapshot do |req|
        snapshot_installed.send(req.files.first.bytes.dup)
      end

      # Pause lagging follower while writing >32 entries to wrap the 32-entry ring buffer
      leader.pause_peer(lagging_follower.node_id, true)
      lagging_follower.pause_peer(leader.node_id, true)

      last_lsn = 0_u64
      40.times do |i|
        e = leader.propose_command(
          LavinMQ::ClusteringV2::LogCommandType::QueueDeclare,
          "overflow.#{i}".to_slice
        )
        last_lsn = e.lsn
      end
      leader.wait_for_commit(last_lsn, 3.seconds).should be_true
      leader.log.first_lsn.should be > 1_u64

      # Resume lagging follower -> leader sees next_idx < log.first_lsn and sends InstallSnapshot
      leader.pause_peer(lagging_follower.node_id, false)
      lagging_follower.pause_peer(leader.node_id, false)

      select
      when snap = snapshot_installed.receive
        String.new(snap).should eq("snapshot-state-v1")
      when timeout(3.seconds)
        fail "Timed out waiting for InstallSnapshot on lagging follower"
      end

      wait_for(2.seconds) { lagging_follower.commit_index >= last_lsn }
      lagging_follower.commit_index.should be >= last_lsn
    ensure
      cluster.cleanup
    end
  end
end
