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
      elected = new_leader || raise "Expected a new leader to be elected"
      elected.node_id.should_not eq(old_leader.node_id)
      elected.current_term.should be > old_leader.current_term
    ensure
      cluster.cleanup
    end
  end

  it "onboards a 4th node as a non-voting Learner and auto-promotes it to Voter once caught up" do
    cluster = ClusteringV2ConsensusSpec::TestCluster.new(3)
    learner_dir = File.tempname("lavinmq_raft_learner_")
    Dir.mkdir_p(learner_dir)
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
      learner = LavinMQ::ClusteringV2::RaftNode.new(
        log: learner_log,
        secret: cluster.secret,
        peer_uri: learner_info.peer_uri,
        amqp_uri: learner_info.amqp_uri,
        http_uri: learner_info.http_uri,
        initial_role: LavinMQ::ClusteringV2::MemberRole::Learner
      )
      begin
        learner.heartbeat_interval = 20.milliseconds
        learner.election_timeout_min = 80.milliseconds
        learner.election_timeout_max = 150.milliseconds
        learner.configure_cluster(seed_peers)
        learner.start("127.0.0.1", learner_port)

        # Add learner to leader; leader will stream log and auto-promote to Voter
        conf_entry = leader.propose_conf_change(
          LavinMQ::ClusteringV2::ConfChangeType::AddLearner,
          learner_info
        )
        leader.wait_for_commit(conf_entry.lsn, 2.seconds).should be_true

        wait_for(3.seconds) do
          leader.members.find { |m| m.node_id == 4_u64 }.try(&.voter?) == true &&
            learner.commit_index >= conf_entry.lsn &&
            learner.role.follower?
        end

        leader.members.find! { |m| m.node_id == 4_u64 }.role.should eq(LavinMQ::ClusteringV2::MemberRole::Voter)
        learner.role.should eq(LavinMQ::ClusteringV2::Role::Follower)
      ensure
        learner.close
      end
    ensure
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
        if (i + 1) % 10 == 0
          leader.wait_for_commit(last_lsn, 3.seconds).should be_true
          wait_for(1.seconds) { leader.last_applied >= last_lsn }
        end
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

  it "ignores learner votes in PreVote and RequestVote so learners cannot form a quorum" do
    base_dir = File.tempname("lavinmq_raft_learner_votes_")
    nodes = Array(LavinMQ::ClusteringV2::RaftNode).new
    begin
      ports = Array(Int32).new(3) do
        srv = TCPServer.new("127.0.0.1", 0)
        port = srv.local_address.port
        srv.close
        port
      end
      # 3 voters (1..3) and 2 learners (4..5); only voter 1 and learners 4..5 are online
      members = [
        LavinMQ::ClusteringV2::MemberInfo.new(1_u64, "tcp://127.0.0.1:#{ports[0]}", "amqp://127.0.0.1:5671", "http://127.0.0.1:15671", LavinMQ::ClusteringV2::MemberRole::Voter),
        LavinMQ::ClusteringV2::MemberInfo.new(2_u64, "tcp://127.0.0.1:65531", "amqp://127.0.0.1:5672", "http://127.0.0.1:15672", LavinMQ::ClusteringV2::MemberRole::Voter),
        LavinMQ::ClusteringV2::MemberInfo.new(3_u64, "tcp://127.0.0.1:65532", "amqp://127.0.0.1:5673", "http://127.0.0.1:15673", LavinMQ::ClusteringV2::MemberRole::Voter),
        LavinMQ::ClusteringV2::MemberInfo.new(4_u64, "tcp://127.0.0.1:#{ports[1]}", "amqp://127.0.0.1:5674", "http://127.0.0.1:15674", LavinMQ::ClusteringV2::MemberRole::Learner),
        LavinMQ::ClusteringV2::MemberInfo.new(5_u64, "tcp://127.0.0.1:#{ports[2]}", "amqp://127.0.0.1:5675", "http://127.0.0.1:15675", LavinMQ::ClusteringV2::MemberRole::Learner),
      ]
      [{1_u64, ports[0], LavinMQ::ClusteringV2::MemberRole::Voter},
       {4_u64, ports[1], LavinMQ::ClusteringV2::MemberRole::Learner},
       {5_u64, ports[2], LavinMQ::ClusteringV2::MemberRole::Learner}].each do |nid, port, role|
        dir = File.join(base_dir, "node-#{nid}")
        Dir.mkdir_p(dir)
        log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 64_u64, node_id: nid)
        node = LavinMQ::ClusteringV2::RaftNode.new(
          log: log,
          secret: "test-secret",
          peer_uri: "tcp://127.0.0.1:#{port}",
          amqp_uri: "amqp://127.0.0.1:567#{nid}",
          http_uri: "http://127.0.0.1:1567#{nid}",
          initial_role: role
        )
        node.heartbeat_interval = 20.milliseconds
        node.election_timeout_min = 60.milliseconds
        node.election_timeout_max = 100.milliseconds
        node.configure_cluster(members)
        node.start("127.0.0.1", port)
        nodes << node
      end

      sleep 250.milliseconds
      # Voter 1 only has itself (1 of 3 voters); learners 4 and 5 must not grant votes or be counted
      nodes.first.leader?.should be_false
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end

  it "does not apply ConfChange from rejected AppendEntries and reverts ConfChange when log is truncated" do
    base_dir = File.tempname("lavinmq_raft_confchange_")
    Dir.mkdir_p(base_dir)
    log = LavinMQ::ClusteringV2::RaftLog.new(base_dir, capacity: 64_u64, node_id: 2_u64)
    srv = TCPServer.new("127.0.0.1", 0)
    port = srv.local_address.port
    srv.close
    node = LavinMQ::ClusteringV2::RaftNode.new(
      log: log,
      secret: "test-secret",
      peer_uri: "tcp://127.0.0.1:#{port}",
      amqp_uri: "amqp://127.0.0.1:5672",
      http_uri: "http://127.0.0.1:15672",
      initial_role: LavinMQ::ClusteringV2::MemberRole::Voter
    )
    begin
      initial_members = [
        LavinMQ::ClusteringV2::MemberInfo.new(1_u64, "tcp://127.0.0.1:65531", "amqp://127.0.0.1:5671", "http://127.0.0.1:15671", LavinMQ::ClusteringV2::MemberRole::Voter),
        LavinMQ::ClusteringV2::MemberInfo.new(2_u64, "tcp://127.0.0.1:#{port}", "amqp://127.0.0.1:5672", "http://127.0.0.1:15672", LavinMQ::ClusteringV2::MemberRole::Voter),
        LavinMQ::ClusteringV2::MemberInfo.new(3_u64, "tcp://127.0.0.1:65532", "amqp://127.0.0.1:5673", "http://127.0.0.1:15673", LavinMQ::ClusteringV2::MemberRole::Voter),
      ]
      node.configure_cluster(initial_members)

      # Build a ConfChange entry adding node 9
      io = IO::Memory.new
      io.write_byte(LavinMQ::ClusteringV2::ConfChangeType::AddLearner.value)
      LavinMQ::ClusteringV2::MemberInfo.new(9_u64, "tcp://127.0.0.1:65533", "amqp://127.0.0.1:5679", "http://127.0.0.1:15679", LavinMQ::ClusteringV2::MemberRole::Learner).to_io(io)
      conf_payload = io.to_slice

      # 1. Rejected AppendEntries (prev_log_lsn = 5 doesn't exist) must NOT apply ConfChange
      rejected_entry = LavinMQ::ClusteringV2::LogEntryRecord.new(
        LavinMQ::ClusteringV2::LogIndexEntry.new(
          term: 1_u64,
          lsn: 6_u64,
          cmd_type: LavinMQ::ClusteringV2::LogCommandType::ConfChange.value
        ),
        conf_payload
      )
      resp = node.handle_append_entries(1_u64, LavinMQ::ClusteringV2::AppendEntriesRequest.new(
        term: 1_u64,
        leader_id: 1_u64,
        prev_log_lsn: 5_u64,
        prev_log_term: 1_u64,
        leader_commit: 0_u64,
        entries: [rejected_entry]
      ))
      (resp || raise "Expected response").success?.should be_false
      node.members.any? { |m| m.node_id == 9_u64 }.should be_false

      # 2. Accepted uncommitted ConfChange at LSN 1 applies, then gets reverted when overwritten by term 2
      accepted_entry = LavinMQ::ClusteringV2::LogEntryRecord.new(
        LavinMQ::ClusteringV2::LogIndexEntry.new(
          term: 1_u64,
          lsn: 1_u64,
          cmd_type: LavinMQ::ClusteringV2::LogCommandType::ConfChange.value
        ),
        conf_payload
      )
      resp2 = node.handle_append_entries(1_u64, LavinMQ::ClusteringV2::AppendEntriesRequest.new(
        term: 1_u64,
        leader_id: 1_u64,
        prev_log_lsn: 0_u64,
        prev_log_term: 0_u64,
        leader_commit: 0_u64,
        entries: [accepted_entry]
      ))
      (resp2 || raise "Expected response").success?.should be_true
      node.members.any? { |m| m.node_id == 9_u64 }.should be_true

      # Overwrite LSN 1 in term 2 with a LeaderNoOp -> ConfChange at LSN 1 is truncated and membership reverts
      noop_entry = LavinMQ::ClusteringV2::LogEntryRecord.new(
        LavinMQ::ClusteringV2::LogIndexEntry.new(
          term: 2_u64,
          lsn: 1_u64,
          cmd_type: LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp.value
        )
      )
      resp3 = node.handle_append_entries(3_u64, LavinMQ::ClusteringV2::AppendEntriesRequest.new(
        term: 2_u64,
        leader_id: 3_u64,
        prev_log_lsn: 0_u64,
        prev_log_term: 0_u64,
        leader_commit: 1_u64,
        entries: [noop_entry]
      ))
      (resp3 || raise "Expected response").success?.should be_true
      node.members.any? { |m| m.node_id == 9_u64 }.should be_false
    ensure
      node.try(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end

  it "invokes on_step_down callback when a higher term is seen in vote, AppendEntries, or InstallSnapshot responses" do
    base_dir = File.tempname("lavinmq_raft_stepdown_cb_")
    Dir.mkdir_p(base_dir)
    log = LavinMQ::ClusteringV2::RaftLog.new(base_dir, capacity: 64_u64, node_id: 1_u64)
    node = LavinMQ::ClusteringV2::RaftNode.new(
      log: log,
      secret: "test-secret",
      peer_uri: "tcp://127.0.0.1:65530",
      amqp_uri: "amqp://127.0.0.1:5671",
      http_uri: "http://127.0.0.1:15671",
      initial_role: LavinMQ::ClusteringV2::MemberRole::Voter
    )
    begin
      node.configure_cluster([
        LavinMQ::ClusteringV2::MemberInfo.new(1_u64, "tcp://127.0.0.1:65530", "amqp://127.0.0.1:5671", "http://127.0.0.1:15671", LavinMQ::ClusteringV2::MemberRole::Voter),
      ])

      stepped_down_terms = Array(UInt64).new
      node.on_step_down do |new_term|
        stepped_down_terms << new_term
      end

      node.trigger_election!
      node.leader?.should be_true
      node.handle_vote_response(2_u64, LavinMQ::ClusteringV2::VoteResponse.new(
        pre_vote: false,
        term: 5_u64,
        voter_id: 2_u64,
        vote_granted: false
      ))
      node.leader?.should be_false
      node.current_term.should eq(5_u64)
      stepped_down_terms.should eq([5_u64])

      node.trigger_election!
      node.leader?.should be_true
      node.handle_append_entries_response(2_u64, LavinMQ::ClusteringV2::AppendEntriesResponse.new(
        term: 10_u64,
        follower_id: 2_u64,
        success: false,
        match_lsn: 0_u64,
        conflict_lsn: 1_u64
      ))
      node.leader?.should be_false
      node.current_term.should eq(10_u64)
      stepped_down_terms.should eq([5_u64, 10_u64])

      node.trigger_election!
      node.leader?.should be_true
      node.handle_install_snapshot_response(2_u64, LavinMQ::ClusteringV2::InstallSnapshotResponse.new(
        term: 15_u64,
        follower_id: 2_u64,
        match_lsn: 0_u64
      ))
      node.leader?.should be_false
      node.current_term.should eq(15_u64)
      stepped_down_terms.should eq([5_u64, 10_u64, 15_u64])
    ensure
      node.close
      FileUtils.rm_rf(base_dir)
    end
  end

  it "uses committed membership rather than uncommitted ConfChange entries for quorum calculation" do
    base_dir = File.tempname("lavinmq_raft_committed_quorum_")
    Dir.mkdir_p(base_dir)
    log = LavinMQ::ClusteringV2::RaftLog.new(base_dir, capacity: 64_u64, node_id: 1_u64)
    node = LavinMQ::ClusteringV2::RaftNode.new(
      log: log,
      secret: "test-secret",
      peer_uri: "tcp://127.0.0.1:65521",
      amqp_uri: "amqp://127.0.0.1:5671",
      http_uri: "http://127.0.0.1:15671",
      initial_role: LavinMQ::ClusteringV2::MemberRole::Voter
    )
    begin
      m1 = LavinMQ::ClusteringV2::MemberInfo.new(1_u64, "tcp://127.0.0.1:65521", "amqp://127.0.0.1:5671", "http://127.0.0.1:15671", LavinMQ::ClusteringV2::MemberRole::Voter)
      m2 = LavinMQ::ClusteringV2::MemberInfo.new(2_u64, "tcp://127.0.0.1:65522", "amqp://127.0.0.1:5672", "http://127.0.0.1:15672", LavinMQ::ClusteringV2::MemberRole::Voter)
      node.configure_cluster([m1, m2])

      # Elect node 1 in term 1 with peer 2's pre-vote and real vote
      node.trigger_election!
      node.handle_vote_response(2_u64, LavinMQ::ClusteringV2::VoteResponse.new(
        pre_vote: true,
        term: 0_u64,
        voter_id: 2_u64,
        vote_granted: true
      ))
      node.handle_vote_response(2_u64, LavinMQ::ClusteringV2::VoteResponse.new(
        pre_vote: false,
        term: 1_u64,
        voter_id: 2_u64,
        vote_granted: true
      ))
      node.leader?.should be_true
      # Peer 2 acknowledges LeaderNoOp at LSN 1
      node.handle_append_entries_response(2_u64, LavinMQ::ClusteringV2::AppendEntriesResponse.new(
        term: 1_u64,
        follower_id: 2_u64,
        success: true,
        match_lsn: 1_u64,
        conflict_lsn: 0_u64
      ))
      node.commit_index.should eq(1_u64)

      # Propose removing voter 2 at LSN 2.
      # Uncommitted @log.members now has only 1 voter (node 1), while @log.committed_members still has 2 voters (1 and 2).
      conf_entry = node.propose_conf_change(LavinMQ::ClusteringV2::ConfChangeType::RemoveNode, m2)
      conf_entry.lsn.should eq(2_u64)
      node.members.map(&.node_id).should eq([1_u64])
      node.committed_members.map(&.node_id).should eq([1_u64, 2_u64])

      # Because committed membership still requires 2 voters for quorum, LSN 2 must NOT commit on node 1 alone!
      node.commit_index.should eq(1_u64)

      # Once peer 2 acknowledges LSN 2, the 2-voter committed quorum is met and LSN 2 commits.
      node.handle_append_entries_response(2_u64, LavinMQ::ClusteringV2::AppendEntriesResponse.new(
        term: 1_u64,
        follower_id: 2_u64,
        success: true,
        match_lsn: 2_u64,
        conflict_lsn: 0_u64
      ))
      node.commit_index.should eq(2_u64)
      node.committed_members.map(&.node_id).should eq([1_u64])
    ensure
      node.close
      FileUtils.rm_rf(base_dir)
    end
  end
end
