require "../../spec_helper"
require "../../../src/lavinmq/clustering/raft/node"
require "../../../src/lavinmq/clustering/raft/transport"

private alias Raft = LavinMQ::Clustering::Raft

private class TCPRaftCluster
  getter nodes = Hash(String, Raft::Node).new
  getter dirs = Array(String).new
  @servers = Hash(String, TCPServer).new
  @transports = Hash(String, Raft::TCPTransport).new
  @addrs : Array(String)

  def initialize(size : Int32, @password = "secret")
    size.times { @servers["x#{@servers.size}"] = TCPServer.new("127.0.0.1", 0) }
    listeners = @servers.values
    @servers.clear
    @addrs = listeners.map { |s| "127.0.0.1:#{s.local_address.port}" }
    @addrs.each_with_index do |addr, i|
      @servers[addr] = listeners[i]
      dirs << (dir = File.tempname("raft-node-spec"))
      Dir.mkdir_p dir
      start(addr, i)
    end
  end

  def start(addr : String, index = @addrs.index!(addr), id = index + 1, dir = dirs[index],
            peers = @addrs) : Raft::Node
    server = @servers[addr]? || (@servers[addr] = TCPServer.new("127.0.0.1", addr.split(':').last.to_i))
    node = @nodes[addr] = Raft::Node.new(id, addr, peers, "tcp://#{addr}", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds, 5.milliseconds, bootstrap: true)
    transport = @transports[addr] = Raft::TCPTransport.new(@password, id, addr, peers.reject(addr),
      ->node.deliver(Raft::TransportEvent))
    spawn transport.listen(server)
    node.run(transport)
    node
  end

  def address(index : Int32) : String
    @addrs[index]
  end

  # Restarts a node on a new port with its data dir. Returns the new address.
  def move(addr : String) : String
    index = @addrs.index!(addr)
    stop(addr)
    server = TCPServer.new("127.0.0.1", 0)
    new_addr = "127.0.0.1:#{server.local_address.port}"
    @addrs[index] = new_addr
    @servers[new_addr] = server
    start(new_addr, index)
    new_addr
  end

  # Starts a node with a new data dir that isn't part of the cluster.
  def start_extra(id : Int32, peers : Array(String), dir = new_dir) : Tuple(String, Raft::Node)
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    @servers[addr] = server
    node = start(addr, 0, id, dir, [addr] + peers)
    {addr, node}
  end

  def new_dir : String
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    dirs << dir
    dir
  end

  def stop(addr : String)
    @servers.delete(addr).try &.close
    @nodes.delete(addr).try &.close
  end

  def leader : Raft::Node?
    @nodes.values.find(&.serving.value)
  end

  def wait_for_leader(except : Raft::Node? = nil) : Raft::Node
    node = nil
    wait_for(5.seconds) { (node = leader) && node != except }
    node.not_nil!
  end

  def close
    @nodes.keys.each { |a| stop(a) }
    dirs.each { |d| FileUtils.rm_rf d }
  end
end

# Answers for every peer as an up to date follower that grants all votes.
# When stalling, the next replicated entries make the node's fiber block past
# the election timeout, with a stale message queued ahead of the acks.
private class StallingTransport < Raft::Transport
  property stall : Time::Span? = nil
  @node : Raft::Node? = nil

  def initialize(@node_ids : Hash(String, Int32))
  end

  def node=(node : Raft::Node)
    @node = node
    @node_ids.each { |addr, id| node.deliver Raft::Connected.new(id, addr) }
  end

  def send(to : String, msg : Raft::Message) : Nil
    node = @node.not_nil!
    id = @node_ids[to]
    case msg
    when Raft::RequestVote
      node.deliver Raft::VoteResponse.new(id, msg.term, true, pre_vote: msg.pre_vote)
    when Raft::AppendEntries
      ack = Raft::AppendResponse.new(id, msg.term, true, msg.prev_index + msg.entries.size)
      if (stall = @stall) && !msg.entries.empty?
        @stall = nil
        node.deliver Raft::AppendResponse.new(id, 0, false, 0)
        node.deliver ack
        ts = LibC::Timespec.new(tv_sec: 0, tv_nsec: stall.total_nanoseconds.to_i64)
        LibC.nanosleep(pointerof(ts), nil) # blocks like a slow fsync
      else
        node.deliver ack
      end
    end
  end

  def close : Nil
  end
end

private def with_raft_cluster(size = 3, &)
  cluster = TCPRaftCluster.new(size)
  yield cluster
ensure
  cluster.try &.close
end

describe Raft::Node do
  it "elects a leader over TCP and replicates the ISR" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      c.nodes.values.count(&.leader?).should eq 1
      uris = c.nodes.values.map(&.leader_uri).uniq!
      uris.size.should eq 1
    end
  end

  it "fails over when the leader stops" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      c.stop(c.nodes.key_for(leader))
      c.wait_for_leader(except: leader)
    end
  end

  it "reports metrics" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      m = leader.metrics.should_not be_nil
      m.is_leader.should be_true
      m.leader.should eq leader.status.not_nil!.id
      m.term.should eq leader.status.not_nil!.term
      m.leader_contact.should eq Time::Span.zero
      m.leader_changes.should eq 1
      m.isr_size.should eq 3
      m.proposals_pending.should eq 0
      m.peers.size.should eq 2
      m.peers.values.should eq [true, true]
      m.save_count.should be > 0
      m.save_buckets.sum.should eq m.save_count
      m.save_buckets.size.should eq Raft::Node::SAVE_BUCKETS.size + 1

      follower = c.nodes.values.find! { |n| n != leader }
      wait_for { follower.metrics.try &.isr_size }
      fm = follower.metrics.should_not be_nil
      fm.is_leader.should be_false
      fm.leader.should eq m.leader
      fm.leader_contact.not_nil!.should be < 1.second

      c.stop(c.nodes.key_for(leader))
      wait_for { follower.metrics.try { |x| x.leader_changes == 2 && x.peers.values.count(false) == 1 } }
    end
  end

  it "hands over leadership on transfer" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      leader.transfer_leadership.should eq Raft::TransferResult::Sent
      wait_for(1.second) { !leader.leader? }
      c.wait_for_leader(except: leader)
    end
  end

  it "reports the seeded membership and refuses changes on a follower" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      status = leader.status.not_nil!
      status.role.leader?.should be_true
      status.membership.not_nil!.voters.should eq Set{1, 2, 3}
      status.membership.not_nil!.addresses.values.to_set.should eq c.nodes.keys.to_set
      status.membership.not_nil!.learners.should be_empty
      status.resolve(c.address(1)).should eq 2
      status.resolve("2").should eq 2
      status.resolve("9").should be_nil
      status.committed_isr.should eq Set{1, 2, 3}
      follower = c.nodes.values.find! { |n| n != leader }
      leader_json = JSON.parse(JSON.build { |j| status.to_json(j) })
      leader_json["leader"].should eq status.address
      leader_json["local"]?.should be_nil
      leader_json["members"].as_a.all?(&.["caught_up"].as_bool?).should be_true
      follower_status = follower.status.not_nil!
      local = JSON.parse(JSON.build { |j| follower_status.to_json(j) })
      local["local"].as_bool.should be_true
      local["role"].should eq "follower"
      local["node"].should eq follower_status.address
      local["leader"].should eq status.address
      local["leader_heard_ago_ms"].as_i64.should be < 1000
      local["members"].as_a.map(&.["match_index"]).uniq!.should eq [nil]
      follower.add_learner("127.0.0.1:1").should eq Raft::MembershipError::NotLeader
      follower.transfer_leadership.should eq Raft::TransferResult::NotLeader
      follower.member?(1).should be_true
      follower.member?(99).should be_false
    end
  end

  it "keeps term and ISR across restarts" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      addr = c.nodes.keys.find! { |a| c.nodes[a] != leader }
      c.stop(addr)
      c.start(addr)
      c.nodes[addr].committed_isr.should eq Set{1, 2, 3}
    end
  end

  it "closes without having been run" do
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    node = Raft::Node.new(1, "127.0.0.1:1", ["127.0.0.1:1"], "tcp://127.0.0.1:1", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds)
    done = Channel(Nil).new
    spawn { node.close; done.close }
    select
    when done.receive?
    when timeout(1.second)
      fail "close hung"
    end
  ensure
    FileUtils.rm_rf dir if dir
  end

  it "handles acks that queued up while stalled before checking the quorum" do
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    transport = StallingTransport.new({"b" => 2, "c" => 3})
    node = Raft::Node.new(1, "a", ["a", "b", "c"], "tcp://a", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds, 5.milliseconds, bootstrap: true)
    transport.node = node
    node.run(transport)
    wait_for { node.serving.value }
    transport.stall = 250.milliseconds
    node.propose_isr(Set{1, 2, 3}).should be_true
    node.serving.value.should be_true
  ensure
    node.try &.close
    FileUtils.rm_rf dir if dir
  end

  it "fails a proposal when not the leader" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      follower = c.nodes.values.find! { |n| n != leader }
      follower.propose_isr(Set{1}).should be_false
    end
  end

  it "doesn't elect with peers using another password" do
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    servers = Array.new(2) { TCPServer.new("127.0.0.1", 0) }
    addrs = servers.map { |s| "127.0.0.1:#{s.local_address.port}" }
    nodes = addrs.map_with_index do |addr, i|
      node = Raft::Node.new(i + 1, addr, addrs, "tcp://#{addr}", Raft::Storage.new(File.join(dir, i.to_s).tap { |d| Dir.mkdir_p d }),
        100.milliseconds, 20.milliseconds, 5.milliseconds, bootstrap: true)
      transport = Raft::TCPTransport.new("password#{i}", i + 1, addr, addrs.reject(addr), ->node.deliver(Raft::TransportEvent))
      spawn transport.listen(servers[i])
      node.run(transport)
      node
    end
    sleep 500.milliseconds
    nodes.none?(&.leader?).should be_true
  ensure
    servers.try &.each &.close
    nodes.try &.each &.close
    FileUtils.rm_rf dir if dir
  end

  it "adds a learner by address, asking it for its clustering id" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      addr, _ = c.start_extra(40, [c.address(0)])
      leader.add_learner(addr).should be_nil
      membership = leader.membership.not_nil!
      membership.learners.should eq Set{40}
      membership.addresses[40].should eq addr
      leader.add_learner("127.0.0.1:1").should eq Raft::MembershipError::Unreachable
    end
  end

  it "takes a node back at a new address with its data dir" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      index = (0..2).find! { |i| c.nodes[c.address(i)] != leader }
      id = index + 1
      new_addr = c.move(c.address(index))
      wait_for(5.seconds) do
        m = leader.membership
        !m.nil? && m.addresses[id]? == new_addr && m.voters.includes?(id)
      end
      wait_for { c.nodes[new_addr].membership.try(&.addresses[id]?) == new_addr }
      c.nodes[new_addr].leader_uri.should eq leader.leader_uri
    end
  end

  it "ignores a copy of a node's data dir while the original is connected" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      index = (0..2).find! { |i| c.nodes[c.address(i)] != leader }
      original = c.address(index)
      copy_dir = c.new_dir
      FileUtils.cp(File.join(c.dirs[index], ".raft_state"), copy_dir)
      _, copy = c.start_extra(index + 1, [c.address(0), c.address(1), c.address(2)], copy_dir)
      sleep 1.second
      membership = leader.membership.not_nil!
      membership.addresses[index + 1].should eq original
      membership.voters.should eq Set{1, 2, 3}
      copy.leader_uri.should be_nil
    end
  end
end

describe Raft::TCPTransport do
  it "lets one address at a time connect with a clustering id" do
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    events = Channel(Raft::TransportEvent).new(16)
    transport = Raft::TCPTransport.new("secret", 1, addr, Array(String).new, ->(e : Raft::TransportEvent) { events.send e })
    spawn transport.listen(server)
    first = Raft::TCPTransport.new("secret", 2, "first:1", [addr], ->(_e : Raft::TransportEvent) { })
    events.receive.should eq Raft::Connected.new(2, "first:1")
    copy = Raft::TCPTransport.new("secret", 2, "copy:1", Array(String).new, ->(_e : Raft::TransportEvent) { })
    copy.probe(addr).should be_nil
    myself = Raft::TCPTransport.new("secret", 1, "other:1", Array(String).new, ->(_e : Raft::TransportEvent) { })
    myself.probe(addr).should be_nil
    first.close
    events.receive.should eq Raft::Disconnected.new(2, "first:1")
    copy.probe(addr).should eq 1
  ensure
    transport.try &.close
    first.try &.close
    copy.try &.close
    myself.try &.close
  end

  it "reconnects to a peer that restarted without anything to send" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port
    addr = "127.0.0.1:#{port}"
    events = Channel(Raft::TransportEvent).new(16)
    handler = ->(e : Raft::TransportEvent) { events.send e }
    peer = Raft::TCPTransport.new("secret", 1, addr, Array(String).new, handler)
    spawn peer.listen(server)
    client = Raft::TCPTransport.new("secret", 2, "client:1", [addr], ->(_e : Raft::TransportEvent) { })
    events.receive.should eq Raft::Connected.new(2, "client:1")
    peer.close
    events.receive.should eq Raft::Disconnected.new(2, "client:1")
    restarted = Raft::TCPTransport.new("secret", 1, addr, Array(String).new, handler)
    spawn restarted.listen(TCPServer.new("127.0.0.1", port, reuse_port: true))
    select
    when event = events.receive
      event.should eq Raft::Connected.new(2, "client:1")
    when timeout(3.seconds)
      fail "didn't reconnect"
    end
  ensure
    client.try &.close
    restarted.try &.close
  end
end

describe Raft::Storage do
  it "round-trips the hard state" do
    with_datadir do |dir|
      storage = Raft::Storage.new(dir)
      storage.load.should be_nil
      state = Raft::HardState.new(7, 2, 3, 6, Set{1, 2}, [Raft::Entry.new(7, nil), Raft::Entry.new(7, Set{2})])
      storage.save(state)
      storage.load.should eq state
    end
  end

  it "round-trips the membership of the snapshot and of the entries" do
    with_datadir do |dir|
      storage = Raft::Storage.new(dir)
      members = Raft::Membership.new(Set{1, 2, 3}, Set{4}, {1 => "a:1", 2 => "b:1", 3 => "c:1", 4 => "d:1"}, Set{4})
      state = Raft::HardState.new(7, nil, 3, 6, Set{1, 2}, [
        Raft::Entry.new(7, nil),
        Raft::Entry.new(7, Set{2}, Raft::Membership.new(Set{1, 2}, Set(Int32).new, {1 => "a:1", 2 => "b:1"})),
      ], members)
      storage.save(state)
      loaded = storage.load.not_nil!
      loaded.should eq state
      loaded.snapshot_membership.should eq members
    end
  end

  it "detects a corrupt state file" do
    with_datadir do |dir|
      storage = Raft::Storage.new(dir)
      storage.save(Raft::HardState.new(7, nil, 0, 0, nil, [] of Raft::Entry))
      bytes = File.read(storage.path).to_slice.dup
      bytes[10] ^= 0xff
      File.write(storage.path, bytes)
      expect_raises(Raft::Storage::CorruptError) { storage.load }
      File.write(storage.path, bytes[0, 5])
      expect_raises(Raft::Storage::CorruptError) { storage.load }
    end
  end
end

describe Raft::Codec do
  it "round-trips every message type" do
    msgs = [
      Raft::RequestVote.new(1, 3, 9, 2, pre_vote: true, transfer: false),
      Raft::VoteResponse.new(2, 3, true, pre_vote: false),
      Raft::AppendEntries.new(1, 3, "tcp://a:5679", 8, 2, [Raft::Entry.new(3, nil), Raft::Entry.new(3, Set{1, 42})], 7),
      Raft::AppendResponse.new(2, 3, false, 5),
      Raft::InstallSnapshot.new(1, 3, "tcp://a:5679", 8, 2, Set{42}),
      Raft::TimeoutNow.new(1, 3),
      Raft::CatchUp.new(2, 3, 8, 2, Set{42}, [Raft::Entry.new(3, Set{7})]),
    ] of Raft::Message
    msgs.each { |m| Raft::Codec.decode(Raft::Codec.encode(m)).should eq m }
  end

  it "round-trips the membership" do
    members = Raft::Membership.new(Set{1, 2}, Set{3}, {1 => "a:5680", 2 => "b:5680", 3 => "c:5680"}, Set{3})
    entries = [Raft::Entry.new(3, nil, members), Raft::Entry.new(3, Set{1}, Raft::Membership.new(Set{1}, Set(Int32).new, {1 => "a:5680"}))]
    msgs = [
      Raft::AppendEntries.new(1, 3, "tcp://a:5679", 8, 2, entries, 7),
      Raft::InstallSnapshot.new(1, 3, "tcp://a:5679", 8, 2, Set{42}, members),
      Raft::CatchUp.new(2, 3, 8, 2, nil, entries, members),
    ] of Raft::Message
    msgs.each { |m| Raft::Codec.decode(Raft::Codec.encode(m)).should eq m }
  end

  it "rejects an implausible member count" do
    membership = Raft::Membership.new(Set{1}, Set(Int32).new, {1 => "a"})
    bytes = Raft::Codec.encode(Raft::InstallSnapshot.new(1, 3, "u", 8, 2, nil, membership))
    # the address count is followed by the one address: id, length and "a"
    offset = bytes.size - (4 + 4 + 4 + 1)
    bytes[offset, 4].copy_from(Bytes[0xff, 0xff, 0xff, 0x7f])
    expect_raises(IO::Error) { Raft::Codec.decode(bytes) }
  end
end
