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

    def initialize(@id, roster, self_uri, @server : TCPServer, data_dir, op : UInt64 = 0u64)
      membership = VRMembership.parse(roster, self_uri)
      @mesh = VRMesh.new(membership, "secret", reconnect_interval: 80.milliseconds)
      @node = VRNode.new(membership, @mesh, VRState.load(data_dir),
        heartbeat_interval: 100.milliseconds, view_change_timeout: 800.milliseconds,
        op_source: -> { op }) # the node's durable log position, for election ordering
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
  end
end
