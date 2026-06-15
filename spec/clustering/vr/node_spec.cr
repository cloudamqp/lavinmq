require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/node"

# In-process VR cluster: each node gets a control mesh over loopback TCP and a
# Node FSM. No data replication or AMQP — the FSM's op/commit sources are stubbed
# — so this exercises leader election and failover in isolation.
module NodeSpec
  alias VRNode = LavinMQ::Clustering::VR::Node
  alias VRMesh = LavinMQ::Clustering::VR::ControlMesh
  alias VRMembership = LavinMQ::Clustering::VR::Membership
  alias VRState = LavinMQ::Clustering::VR::State

  class TestNode
    getter node : VRNode
    getter mesh : VRMesh
    getter id : Int32

    @stepped_down = Atomic(Int32).new(0)

    def initialize(@id, roster, self_uri, @server : TCPServer, data_dir, op : UInt64 = 0u64)
      membership = VRMembership.parse(roster, self_uri)
      @mesh = VRMesh.new(membership, "secret", reconnect_interval: 80.milliseconds)
      @node = VRNode.new(membership, @mesh, VRState.load(data_dir),
        heartbeat_interval: 100.milliseconds, view_change_timeout: 800.milliseconds,
        op_source: -> { op }, # the node's durable log position, for election ordering
        on_step_down: -> { @stepped_down.set(1) })
    end

    # True once this node, having been primary, has stepped down (the analogue of
    # the runner's exit-3-and-rejoin path).
    def stepped_down? : Bool
      @stepped_down.get == 1
    end

    def start
      spawn(name: "node#{@id} accept") do
        while socket = @server.accept?
          spawn { @mesh.handle_accept(socket) }
        end
      end
      @mesh.start
      @node.start
    end

    def close
      @node.close
      @mesh.close
      @server.close rescue nil
    end
  end

  # The single id all live nodes agree is primary, with exactly one claiming the
  # role — or nil if they haven't converged.
  private def self.agreed_primary(live : Array(TestNode)) : Int32?
    claimers = live.count(&.node.primary?)
    return nil unless claimers == 1
    p = live.find!(&.node.primary?).node.self_id
    return nil unless live.all?(&.node.primary_id.== p)
    p
  end

  describe LavinMQ::Clustering::VR::Node do
    it "converges on one agreed primary and fails over to another on its loss" do
      with_datadir do |base|
        servers = (1..3).map { TCPServer.new("127.0.0.1", 0) }
        ports = servers.map(&.local_address.port)
        roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{ports[i - 1]}" }.join(",")

        nodes = (1..3).map do |i|
          dir = File.join(base, "n#{i}")
          Dir.mkdir_p dir
          TestNode.new(i, roster, "tcp://127.0.0.1:#{ports[i - 1]}", servers[i - 1], dir)
        end
        nodes.each &.start

        # All three converge on one agreed primary.
        wait_for(10.seconds) { agreed_primary(nodes) }
        first = agreed_primary(nodes).not_nil!

        # The status snapshot (HTTP leader-discovery endpoint) agrees: exactly
        # one node reports role "primary", and all point at the same primary_id.
        leader_status = nodes.find!(&.node.self_id.== first).node.status
        leader_status[:role].should eq "primary"
        leader_status[:primary_id].should eq first
        nodes.each { |n| n.node.status[:primary_id].should eq first }

        # Kill the elected primary; the two survivors must elect a new agreed
        # primary (necessarily a different node), preserving CP — a majority is
        # still present.
        killed = nodes.find!(&.node.self_id.== first)
        survivors = nodes.reject(&.node.self_id.== first)
        killed.close

        wait_for(10.seconds) do
          p = agreed_primary(survivors)
          p && p != first
        end
        second = agreed_primary(survivors).not_nil!
        second.should_not eq first
        survivors.find!(&.node.self_id.== second).node.view.should be > 0
      ensure
        nodes.try &.each { |n| n.close rescue nil }
      end
    end

    # Regression for the data-loss bug: an under-replicated node (e.g. one that
    # just restarted with an empty/stale data dir) reports a low op and MUST NOT
    # win the election just because it has the lowest id — otherwise it becomes
    # primary and full_sync wipes the more-complete nodes' data. The most
    # up-to-date node must win regardless of id.
    it "elects the most up-to-date node, never an under-replicated lower-id node" do
      with_datadir do |base|
        servers = (1..3).map { TCPServer.new("127.0.0.1", 0) }
        ports = servers.map(&.local_address.port)
        roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{ports[i - 1]}" }.join(",")

        # Node 1 (lowest id) is "empty" (op 0); nodes 2 and 3 hold data (op 5).
        ops = {1 => 0u64, 2 => 5u64, 3 => 5u64}
        nodes = (1..3).map do |i|
          dir = File.join(base, "n#{i}")
          Dir.mkdir_p dir
          TestNode.new(i, roster, "tcp://127.0.0.1:#{ports[i - 1]}", servers[i - 1], dir, op: ops[i])
        end
        nodes.each &.start

        wait_for(10.seconds) { agreed_primary(nodes) }
        primary = agreed_primary(nodes).not_nil!
        primary.should_not eq 1 # the empty lowest-id node must not lead
        primary.should eq 2     # most up-to-date, tie-broken to lowest id (2 over 3)
      ensure
        nodes.try &.each { |n| n.close rescue nil }
      end
    end

    # Regression for split-brain: a primary that loses contact with a quorum must
    # step down (the runner then exits and rejoins as a backup). The etcd lease
    # used to provide this; the VR primary must self-fence on its tick instead,
    # or it would keep serving reads / unconfirmed writes as a stale leader while
    # the partitioned-away majority elects a new one.
    it "steps down a primary that can no longer reach a quorum" do
      with_datadir do |base|
        servers = (1..3).map { TCPServer.new("127.0.0.1", 0) }
        ports = servers.map(&.local_address.port)
        roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{ports[i - 1]}" }.join(",")

        nodes = (1..3).map do |i|
          dir = File.join(base, "n#{i}")
          Dir.mkdir_p dir
          TestNode.new(i, roster, "tcp://127.0.0.1:#{ports[i - 1]}", servers[i - 1], dir)
        end
        nodes.each &.start

        wait_for(10.seconds) { agreed_primary(nodes) }
        primary_id = agreed_primary(nodes).not_nil!
        primary = nodes.find!(&.node.self_id.== primary_id)
        survivors = nodes.reject(&.node.self_id.== primary_id)

        # Cut the primary off from BOTH peers: it is now 1 of 3, below quorum(2).
        survivors.each &.close

        # After connectivity stays below quorum for a view-change timeout, the
        # isolated primary must step down rather than keep serving.
        wait_for(10.seconds) { primary.stepped_down? }
        primary.stepped_down?.should be_true
      ensure
        nodes.try &.each { |n| n.close rescue nil }
      end
    end

    # Regression for the behind-leader reconnect wedge: when the data layer
    # rejects the current primary as behind our committed data (Client#sync raises
    # BehindLeaderError), it calls Node#request_view_change so a more-complete node
    # can take over — instead of the follower reconnecting to the same unfit
    # primary forever. The call must put a backup into a new view change.
    it "request_view_change makes a backup start a new election" do
      with_datadir do |base|
        server = TCPServer.new("127.0.0.1", 0)
        port = server.local_address.port
        # A 3-node roster, but only this node runs, so it never learns a primary
        # and stays a backup — the state the wedged follower would be in.
        roster = "1=tcp://127.0.0.1:#{port},2=tcp://127.0.0.1:1,3=tcp://127.0.0.1:2"
        dir = File.join(base, "n1")
        Dir.mkdir_p dir
        node = TestNode.new(1, roster, "tcp://127.0.0.1:#{port}", server, dir)
        node.start
        node.node.primary?.should be_false
        before = node.node.view

        node.node.request_view_change

        node.node.status[:role].should eq "view_change"
        node.node.view.should be > before
      ensure
        node.try &.close
      end
    end

    # Regression: the deterministic decider for a view (primary_of(view)) may be
    # the one node that's down. A live majority must advance to a later view whose
    # decider is up, instead of rebroadcasting StartViewChange for the dead
    # decider's view forever — otherwise the cluster stays leaderless despite a
    # quorum being present.
    it "advances past an unavailable decider to elect with the remaining quorum" do
      with_datadir do |base|
        servers = (1..3).map { TCPServer.new("127.0.0.1", 0) }
        ports = servers.map(&.local_address.port)
        roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{ports[i - 1]}" }.join(",")

        nodes = (1..3).map do |i|
          dir = File.join(base, "n#{i}")
          Dir.mkdir_p dir
          TestNode.new(i, roster, "tcp://127.0.0.1:#{ports[i - 1]}", servers[i - 1], dir)
        end

        # Node 2 is the decider for the first election (view 1 -> primary_of(1) =
        # members[1] = id 2). Keep it down; the live majority {1,3} must still
        # converge on a primary by rotating to a view with a reachable decider.
        servers[1].close
        live = [nodes[0], nodes[2]]
        live.each &.start

        wait_for(15.seconds) { agreed_primary(live) }
        agreed_primary(live).should_not be_nil
      ensure
        nodes.try &.each { |n| n.close rescue nil }
      end
    end
  end
end
