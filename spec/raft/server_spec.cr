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
        server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{3})).should be_true
        deadline = Time.instant + 2.seconds
        until server.state.isr == Set{3}
          fail "timed out" if Time.instant > deadline
          Fiber.yield
        end
        server.isr.should eq Set{3}
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

  describe "concurrent shutdown" do
    it "never leaves a run_on_tick caller blocked when the server stops" do
      dir = tmp_data_dir
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      transport = ::Raft::MemoryTransport.new(1_u64)
      server = LavinMQ::Raft::Server.new(
        data_dir: dir, advertised_address: "n:5680,n:5679",
        transport: transport, execution_context: Fiber::ExecutionContext.current,
      )
      transport.start
      server.start
      begin
        proposers = 20
        done = ::Channel(Symbol).new(proposers)
        proposers.times do
          spawn do
            # Either returns a Bool or raises "stopping" — but must never hang
            # waiting on a reply the tick loop will never deliver after stop.
            server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1}))
            done.send(:ok)
          rescue
            done.send(:raised)
          end
        end
        Fiber.yield # let proposers enqueue their actions before we stop
        server.stop
        proposers.times do
          select
          when done.receive
          when timeout(5.seconds)
            fail "a run_on_tick caller hung after the server stopped"
          end
        end
      ensure
        transport.stop
        FileUtils.rm_rf(dir)
      end
    end
  end
end
