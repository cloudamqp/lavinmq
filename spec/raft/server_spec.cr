require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/server"

private def tmp_data_dir : String
  dir = File.tempname("raft-server-spec")
  Dir.mkdir_p(dir)
  dir
end

private def with_single_node(&)
  dir = tmp_data_dir
  File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
  transport = ::Raft::MemoryTransport.new(1_u64)
  server = LavinMQ::Raft::Server.new(
    data_dir: dir,
    advertised_address: "node1:5680,node1:5679",
    transport: transport,
    execution_context: Fiber::ExecutionContext.current,
  )
  transport.start
  server.start
  begin
    yield server
  ensure
    server.stop
    transport.stop
    FileUtils.rm_rf(dir)
  end
end

private def with_cluster(node_count : Int32 = 3, &)
  ids = (1..node_count).map(&.to_u64)
  transports = ::Raft::MemoryTransport.mesh(ids)
  dirs = [] of String
  servers = ids.map do |id|
    dir = tmp_data_dir
    dirs << dir
    File.write(File.join(dir, ".clustering_id"), id.to_s(36))
    LavinMQ::Raft::Server.new(
      data_dir: dir,
      advertised_address: "node#{id}:5680,node#{id}:5679",
      transport: transports[id],
      execution_context: Fiber::ExecutionContext.current,
    )
  end
  transports.each_value(&.start)
  servers.each(&.start)
  begin
    yield transports, servers
  ensure
    transports.each_value(&.stop)
    servers.each(&.stop)
    dirs.each { |d| FileUtils.rm_rf(d) }
  end
end

private def retry_until(timeout : Time::Span = 2.seconds, &block : -> Bool)
  deadline = Time.instant + timeout
  until block.call
    fail "operation timed out" if Time.instant > deadline
    Fiber.yield
  end
end

# Bootstraps servers[0] and adds each remaining node. raft.cr auto-promotes
# learners to voters once they catch up, so we just add and then wait for the
# voting set to include every node. Waits until exactly one server reports
# itself leader. Returns the leader.
private def form_cluster(servers) : LavinMQ::Raft::Server
  leader = servers.first
  leader.bootstrap.should be_true
  servers[1..].each do |s|
    addr = "node#{s.node_id}:5680,node#{s.node_id}:5679"
    retry_until(5.seconds) { leader.add_server(s.node_id, addr) }
  end
  retry_until(5.seconds) { servers.all? { |s| leader.voters.includes?(s.node_id.to_u64) } }
  result = nil
  deadline = Time.instant + 5.seconds
  until result
    leaders = servers.select(&.is_leader.value)
    result = leaders.first if leaders.size == 1
    fail "timed out waiting for a single leader (had #{leaders.size})" if Time.instant > deadline
    Fiber.yield unless result
  end
  result.not_nil!
end

describe LavinMQ::Raft::Server do
  describe "node_id" do
    it "generates and persists across reconstruction with the same data_dir" do
      dir = tmp_data_dir
      begin
        t1 = ::Raft::MemoryTransport.new(1_u64)
        s1 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t1,
        )
        first_id = s1.node_id
        s1.stop

        t2 = ::Raft::MemoryTransport.new(1_u64)
        s2 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t2,
        )
        s2.node_id.should eq first_id
        s2.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end

  describe "peer address cache" do
    # Regression: recover_state restores @node.peers from disk but never fires
    # on_configuration_applied, so a restarted node must seed its peer-address
    # cache from the recovered config — otherwise it can never resolve the
    # leader to follow (Jepsen: followers stuck "waiting to be added to ISR").
    it "is seeded from the recovered config on restart (no config entry applied)" do
      dir = tmp_data_dir
      begin
        File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
        t1 = ::Raft::MemoryTransport.new(1_u64)
        s1 = LavinMQ::Raft::Server.new(
          data_dir: dir, advertised_address: "node1:5680,node1:5679",
          transport: t1, execution_context: Fiber::ExecutionContext.current)
        t1.start
        s1.start
        s1.bootstrap.should be_true
        select
        when s1.is_leader.when_true.receive
        when timeout(2.seconds); fail "node 1 did not become leader"
        end
        # Add a peer so the config (carrying its address) is persisted to disk.
        retry_until(5.seconds) { s1.add_server(2, "node2:5680,node2:5679") }
        retry_until(5.seconds) { !s1.peer_address(2_u64).nil? } # populated live
        s1.stop
        t1.stop

        # Restart with the same data_dir: recover_state loads peers, but fires
        # no callback — start must seed the cache so the address resolves now.
        t2 = ::Raft::MemoryTransport.new(1_u64)
        s2 = LavinMQ::Raft::Server.new(
          data_dir: dir, advertised_address: "node1:5680,node1:5679",
          transport: t2, execution_context: Fiber::ExecutionContext.current)
        t2.start
        s2.start
        addr = s2.peer_address(2_u64)
        addr.should_not be_nil
        addr.not_nil!.data_uri.should eq "tcp://node2:5679"
        s2.stop
        t2.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end

  describe "single node" do
    it "stays follower until bootstrap" do
      with_single_node do |server|
        Fiber.yield # let the tick loop run at least once
        server.is_leader.value.should be_false
      end
    end

    it "becomes leader after bootstrap and fires is_leader.when_true" do
      with_single_node do |server|
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
          # became leader
        when timeout(2.seconds)
          fail "timed out waiting for leadership"
        end
        server.is_leader.value.should be_true
      end
    end

    it "applies a proposed command to the state machine" do
      with_single_node do |server|
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
          # leader
        when timeout(2.seconds)
          fail "timed out waiting for leadership"
        end

        server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{7})).should be_true

        deadline = Time.instant + 2.seconds
        until server.state_machine.isr.includes?(7)
          fail "timed out waiting for apply" if Time.instant > deadline
          Fiber.yield
        end
      end
    end
  end

  describe "three-node cluster" do
    it "elects exactly one leader" do
      with_cluster(3) do |_transports, servers|
        form_cluster(servers)
        servers.count(&.is_leader.value).should eq 1
      end
    end

    it "agrees on leader_id across all nodes" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        retry_until { servers.all? { |s| s.leader_id == leader.node_id.to_u64 } }
      end
    end

    it "replicates a proposed command to all state machines" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        leader.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{42})).should be_true
        retry_until(5.seconds) { servers.all?(&.state_machine.isr.includes?(42)) }
      end
    end

    it "returns false when proposing on a follower" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        follower = servers.find! { |s| s != leader }
        follower.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{99})).should be_false
      end
    end

    it "caches each configured peer's parsed address after configuration applies" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        other = servers.find! { |s| s != leader }
        addr = leader.peer_address(other.node_id.to_u64)
        addr.should_not be_nil
        addr.not_nil!.data_uri.should eq "tcp://node#{other.node_id}:5679"
      end
    end

    it "exposes peers as a consistent snapshot (not the live @node.peers)" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        ids = leader.peers.map(&.id).to_set
        ids.should eq servers.map(&.node_id.to_u64).to_set
        # Snapshot is replaced wholesale, so a held reference stays stable.
        leader.peers.should_not be(leader.@node.peers)
      end
    end
  end

  describe "leadership loss" do
    it "elects a new leader after the current leader is isolated, and the old leader steps down on heal" do
      with_cluster(3) do |transports, servers|
        old_leader = form_cluster(servers)

        # Isolate the leader; the other two should elect a new one.
        transports[old_leader.node_id.to_u64].isolated = true
        new_leader = nil
        retry_until(5.seconds) do
          candidates = servers.select { |s| s != old_leader && s.is_leader.value }
          new_leader = candidates.first if candidates.size == 1
          !new_leader.nil?
        end

        # Heal the partition; the old leader sees a higher term and steps down.
        transports[old_leader.node_id.to_u64].isolated = false
        select
        when old_leader.is_leader.when_false.receive
          # expected: stepped down on seeing the new leader's higher term
        when timeout(5.seconds)
          fail "old leader did not step down after heal"
        end
        old_leader.is_leader.value.should be_false
      end
    end
  end

  describe "transfer_leadership" do
    it "hands leadership to a follower voter" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        follower = servers.find! { |s| s != leader }
        leader.transfer_leadership(to: follower.node_id).should be_true
        retry_until(5.seconds) { follower.is_leader.value }
      end
    end

    it "returns false when called on a follower" do
      with_cluster(3) do |_transports, servers|
        leader = form_cluster(servers)
        follower = servers.find! { |s| s != leader }
        follower.transfer_leadership(to: leader.node_id).should be_false
      end
    end
  end

  describe "state accessors" do
    it "exposes the state-machine snapshot via Server#state" do
      with_single_node do |server|
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
        when timeout(2.seconds)
          fail "timed out"
        end
        server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{7})).should be_true
        deadline = Time.instant + 2.seconds
        until server.state.isr.includes?(7)
          fail "timed out" if Time.instant > deadline
          Fiber.yield
        end
        server.isr.should eq Set{7}
      end
    end
  end

  describe "propose_committed" do
    it "returns true once the entry commits under the proposing term" do
      with_single_node do |server|
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
        when timeout(2.seconds); fail "not leader"
        end
        server.propose_committed(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1})).should be_true
        server.isr.should eq Set{1}
      end
    end

    it "returns false when proposing on a non-leader" do
      with_cluster(3) do |_t, servers|
        leader = form_cluster(servers)
        follower = servers.find! { |s| s != leader }
        follower.propose_committed(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{9})).should be_false
      end
    end
  end

  describe "stop" do
    it "unblocks an in-flight run_on_tick caller instead of hanging" do
      with_single_node do |server|
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
        when timeout(2.seconds); fail "not leader"
        end
        result = nil.as(Bool?)
        done = Channel(Nil).new
        spawn do
          result = server.propose_committed(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{2}))
          done.close
        end
        Fiber.yield
        server.stop
        select
        when done.receive?
        when timeout(2.seconds); fail "propose_committed hung across stop()"
        end
        result.should_not be_nil
      end
    end
  end

  describe "on_leader_change" do
    it "fires the callback when a single node bootstraps to leader" do
      observed = [] of UInt64?
      with_single_node do |server|
        server.on_leader_change { |id| observed << id }
        server.bootstrap.should be_true
        deadline = Time.instant + 2.seconds
        until observed.includes?(server.node_id.to_u64)
          fail "timed out waiting for leader-change callback" if Time.instant > deadline
          Fiber.yield
        end
      end
    end
  end

  describe "on_configuration_change" do
    # Regression for the follower-never-follows gap: a joining follower learns
    # leader_id from a heartbeat before the configuration entry carrying the
    # leader's address is applied, so its one-shot leader-change resolves to a
    # nil address. on_configuration_change re-fires once addresses are known,
    # carrying the current leader_id, so the follow path can resolve + connect.
    it "fires with the cluster peers after the configuration applies" do
      with_cluster(2) do |_transports, servers|
        saw_remote_peer = Channel(Nil).new(1)
        servers.each do |s|
          s.on_configuration_change do |peers|
            # Carries the applied config; a peer other than self is the address
            # info a freshly-joined follower needs to resolve and follow.
            # try_send? (non-blocking): this runs on the tick fiber, which a
            # blocking send on a full channel would freeze (stalling raft).
            saw_remote_peer.try_send?(nil) if peers.any? { |p| p.id != s.node_id.to_u64 }
          end
        end
        form_cluster(servers)
        select
        when saw_remote_peer.receive
          # expected
        when timeout(5.seconds)
          fail "on_configuration_change never delivered the cluster peers after the config applied"
        end
      end
    end
  end

  describe "node_id sources" do
    it "reads .clustering_id when present (base-36)" do
      dir = tmp_data_dir
      begin
        # "9ix" base-36 == 12345 decimal
        File.write(File.join(dir, ".clustering_id"), "9ix")
        transport = ::Raft::MemoryTransport.new(12345_u64)
        server = LavinMQ::Raft::Server.new(
          data_dir: dir, advertised_address: "n:5680,n:5679", transport: transport,
        )
        server.node_id.should eq 12345
        server.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end

    it "generates a fresh id when no file exists" do
      dir = tmp_data_dir
      begin
        transport = ::Raft::MemoryTransport.new(0_u64)
        server = LavinMQ::Raft::Server.new(
          data_dir: dir, advertised_address: "n:5680,n:5679", transport: transport,
        )
        server.node_id.should be > 0
        File.exists?(File.join(dir, ".clustering_id")).should be_true
        server.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end

    it "raises a clear error on a corrupt .clustering_id instead of regenerating" do
      dir = tmp_data_dir
      begin
        File.write(File.join(dir, ".clustering_id"), "not-base-36-#")
        transport = ::Raft::MemoryTransport.new(1_u64)
        expect_raises(Exception, /Invalid cluster id/) do
          LavinMQ::Raft::Server.new(
            data_dir: dir, advertised_address: "n:5680,n:5679", transport: transport,
          )
        end
      ensure
        FileUtils.rm_rf(dir)
      end
    end

    it "raises on an empty .clustering_id rather than silently picking a new identity" do
      dir = tmp_data_dir
      begin
        File.write(File.join(dir, ".clustering_id"), "")
        transport = ::Raft::MemoryTransport.new(1_u64)
        expect_raises(Exception, /Invalid cluster id/) do
          LavinMQ::Raft::Server.new(
            data_dir: dir, advertised_address: "n:5680,n:5679", transport: transport,
          )
        end
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end
end
