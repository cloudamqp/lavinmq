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
      100.milliseconds, 20.milliseconds)
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

  # Restarts every node at once on new ports, with their data dirs and
  # seeds listing the new addresses. Returns the new addresses.
  def move_all : Array(String)
    @addrs.each { |a| stop(a) }
    servers = @addrs.map { TCPServer.new("127.0.0.1", 0) }
    @addrs = servers.map { |s| "127.0.0.1:#{s.local_address.port}" }
    @addrs.each_with_index { |a, i| @servers[a] = servers[i] }
    @addrs.each_with_index { |a, i| start(a, i) }
    @addrs
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
    node.should_not(be_nil)
  end

  # Commits `isr` from whoever leads by then: with a 100 ms election timeout
  # a loaded machine can move leadership between finding the leader and the
  # commit. Returns the leader that committed it.
  def propose_isr(isr : Set(Int32)) : Raft::Node
    node = nil
    wait_for(5.seconds) { (node = wait_for_leader).propose_isr(isr) }
    node.should_not(be_nil)
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
  # When false, followers answer appends without taking any entries
  property? ack = true
  # When true, followers don't answer appends at all, but still vote
  property? drop_appends = false
  @node : Raft::Node? = nil

  def initialize(@node_ids : Hash(String, Int32))
  end

  def node=(node : Raft::Node)
    @node = node
    @node_ids.each { |addr, id| node.deliver Raft::Connected.new(id, addr) }
  end

  def send(to : String, msg : Raft::Message) : Nil
    node = @node.should_not(be_nil)
    id = @node_ids[to]
    case msg
    when Raft::SeedInfo
      node.deliver Raft::SeedInfo.new(id, 0, 0, has_data: false, prior_in_sync: false)
    when Raft::RequestVote
      node.deliver Raft::VoteResponse.new(id, msg.term, true, pre_vote: msg.pre_vote)
    when Raft::AppendEntries
      return if drop_appends?
      unless ack?
        # From another fiber: the leader resends right away and would fill
        # its event queue from its own fiber
        nack = Raft::AppendResponse.new(id, msg.term, true, msg.prev_index)
        spawn { sleep 5.milliseconds; node.deliver nack }
        return
      end
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

# Runs a membership change on the current leader, again on the next one if
# leadership moved first
private def on_current_leader(cluster, & : Raft::Node -> Raft::MembershipError?) : Raft::MembershipError?
  result = nil
  wait_for(5.seconds) do
    result = yield cluster.wait_for_leader
    !result.in?(Raft::MembershipError::NotLeader, Raft::MembershipError::NotServing)
  end
  result
end

describe Raft::Node do
  it "elects a leader over TCP and replicates the ISR" do
    with_raft_cluster do |c|
      c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      c.nodes.values.count(&.leader?).should eq 1
      uris = c.nodes.values.map(&.leader_uri).uniq!
      uris.size.should eq 1
    end
  end

  it "fails over when the leader stops" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      c.stop(c.nodes.key_for(leader))
      c.wait_for_leader(except: leader)
    end
  end

  it "reports metrics" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      # With a 100 ms election timeout a loaded machine can move leadership
      # on its own, so wait for a settled view rather than read it once
      m = nil
      wait_for do
        leader = c.wait_for_leader
        s = leader.status
        x = leader.metrics
        m = x if s && x && x.is_leader && x.leader == s.id && x.term == s.term && x.isr_size == 3 &&
                 x.proposals_pending == 0 && x.peers.size == 2 && x.peers.values.all?
      end
      m = m.should_not(be_nil)
      m.leader_contact.should eq Time::Span.zero
      m.leader_changes.should be >= 1
      m.save_count.should be > 0
      m.save_buckets.sum.should eq m.save_count
      m.save_buckets.size.should eq Raft::Node::SAVE_BUCKETS.size + 1

      follower = c.nodes.values.find! { |n| n != leader }
      fm = nil
      wait_for { fm = follower.metrics.try { |x| x if !x.is_leader && x.leader == m.leader && x.isr_size } }
      fm = fm.should_not(be_nil)
      fm.leader_contact.should_not(be_nil).should be < 1.second

      c.stop(c.nodes.key_for(leader))
      wait_for do
        follower.metrics.try do |x|
          x.leader_changes > fm.leader_changes && x.leader != m.leader && x.peers.values.count(false) == 1
        end
      end
    end
  end

  it "counts the entries a leader appended itself among pending proposals" do
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    transport = StallingTransport.new({"b" => 2, "c" => 3})
    transport.ack = false
    node = Raft::Node.new(1, "a", ["a", "b", "c"], "tcp://a", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds)
    transport.node = node
    node.run(transport)
    # Its seeded membership, which no caller proposed, can't commit
    wait_for { node.metrics.try &.is_leader }
    node.metrics.should_not(be_nil).proposals_pending.should be > 0
  ensure
    node.try &.close
    FileUtils.rm_rf dir if dir
  end

  it "counts the same node elected again in a later term as a leader change" do
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    transport = StallingTransport.new({"b" => 2, "c" => 3})
    node = Raft::Node.new(1, "a", ["a", "b", "c"], "tcp://a", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds)
    transport.node = node
    node.run(transport)
    wait_for { node.serving.value }
    first = node.metrics.should_not(be_nil)
    first.leader_changes.should eq 1
    # No followers answering: it steps down on losing its quorum, and the
    # votes they still grant elect it again
    transport.drop_appends = true
    wait_for { node.metrics.try { |m| m.is_leader && m.term > first.term && m.leader_changes > 1 } }
  ensure
    node.try &.close
    FileUtils.rm_rf dir if dir
  end

  {"doesn't answer" => 0, "doesn't take requests" => Raft::Node::EVENT_QUEUE_SIZE}.each do |stuck, queued|
    it "gives up on status and metrics when the event loop #{stuck}" do
      dir = File.tempname("raft-node-spec")
      Dir.mkdir_p dir
      # Not run, like an event loop stuck in an fsync while events queue up
      node = Raft::Node.new(1, "127.0.0.1:1", ["127.0.0.1:1"], "tcp://127.0.0.1:1", Raft::Storage.new(dir),
        100.milliseconds, 20.milliseconds)
      queued.times { node.deliver Raft::Disconnected.new(2, "127.0.0.1:2") }
      gave_up = Channel(Bool).new(2)
      spawn { gave_up.send node.status.nil? }
      spawn { gave_up.send node.metrics.nil? }
      2.times do
        select
        when nil_answer = gave_up.receive
          nil_answer.should be_true
        when timeout(3.seconds)
          fail "a reader hung"
        end
      end
    ensure
      FileUtils.rm_rf dir if dir
    end
  end

  it "hands over leadership on transfer" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      # With a 100 ms election timeout a loaded machine can move leadership
      # on its own, so transfer from whoever leads by then
      wait_for do
        leader = c.wait_for_leader
        leader.transfer_leadership.sent?
      end
      wait_for(1.second) { !leader.leader? }
      c.wait_for_leader(except: leader)
    end
  end

  it "reports the seeded membership and refuses changes on a follower" do
    with_raft_cluster do |c|
      c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      # With a 100 ms election timeout a loaded machine can move leadership
      # on its own, so read a leader's and a follower's status that agree
      follower = nil
      status = follower_status = nil
      wait_for do
        leader = c.wait_for_leader
        follower = c.nodes.values.find! { |n| n != leader }
        status = leader.status
        follower_status = follower.status
        status && follower_status && status.role.leader? && follower_status.role.follower? &&
          follower_status.leader == status.id
      end
      status = status.should_not(be_nil)
      follower_status = follower_status.should_not(be_nil)
      follower = follower.should_not(be_nil)
      status.role.leader?.should be_true
      status.membership.should_not(be_nil).voters.should eq Set{1, 2, 3}
      status.membership.should_not(be_nil).addresses.values.to_set.should eq c.nodes.keys.to_set
      status.membership.should_not(be_nil).learners.should be_empty
      status.resolve(c.address(1)).should eq 2
      status.resolve("2").should eq 2
      status.resolve("9").should be_nil
      status.committed_isr.should eq Set{1, 2, 3}
      leader_json = JSON.parse(JSON.build { |j| status.to_json(j) })
      leader_json["leader"].should eq status.address
      leader_json["local"]?.should be_nil
      leader_json["members"].as_a.all?(&.["caught_up"].as_bool?).should be_true
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
      leader = c.propose_isr(Set{1, 2, 3})
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
      100.milliseconds, 20.milliseconds)
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
        100.milliseconds, 20.milliseconds)
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
      c.propose_isr(Set{1, 2, 3})
      addr, _ = c.start_extra(40, [c.address(0)])
      # With its 100 ms election timeout a loaded runner can move leadership
      # between finding the leader and asking it, so ask whoever leads then
      on_current_leader(c, &.add_learner(addr)).should be_nil
      membership = c.wait_for_leader.membership.should_not(be_nil)
      membership.learners.should eq Set{40}
      membership.addresses[40].should eq addr
      on_current_leader(c, &.add_learner("127.0.0.1:1")).should eq Raft::MembershipError::Unreachable
    end
  end

  it "takes a node back at a new address with its data dir" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      index = (0..2).find! { |i| c.nodes[c.address(i)] != leader }
      id = index + 1
      new_addr = c.move(c.address(index))
      wait_for(5.seconds) do
        m = leader.membership
        !m.nil? && m.addresses[id]? == new_addr && m.voters.includes?(id)
      end
      wait_for { c.nodes[new_addr].membership.try(&.addresses[id]?) == new_addr }
      # Leadership may have moved meanwhile, follow whoever leads by then
      wait_for { c.nodes[new_addr].leader_uri == c.wait_for_leader.leader_uri }
    end
  end

  it "elects a leader after every node moved at once, through the seeds" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      new_addrs = c.move_all
      leader = c.wait_for_leader
      wait_for(5.seconds) do
        m = leader.membership
        !m.nil? && m.voters.size == 3 && (1..3).all? { |id| m.addresses[id]? == new_addrs[id - 1] }
      end
    end
  end

  it "leads a single node cluster whose only seed names it differently" do
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    seeds = ["localhost:#{server.local_address.port}"]
    dir = File.tempname("raft-node-spec")
    Dir.mkdir_p dir
    node = Raft::Node.new(1, addr, seeds, "tcp://#{addr}", Raft::Storage.new(dir),
      100.milliseconds, 20.milliseconds)
    transport = Raft::TCPTransport.new("secret", 1, addr, seeds, ->node.deliver(Raft::TransportEvent))
    spawn transport.listen(server)
    node.run(transport)
    wait_for(5.seconds) { node.serving.value }
    node.membership.should eq Raft::Membership.new(Set{1}, Set(Int32).new, {1 => addr})
  ensure
    node.try &.close
    server.try &.close
    FileUtils.rm_rf(dir) if dir
  end

  it "ignores a copy of a node's data dir while the original is connected" do
    with_raft_cluster do |c|
      leader = c.propose_isr(Set{1, 2, 3})
      wait_for { c.nodes.values.all? { |n| n.membership.try(&.voters.size) == 3 } }
      index = (0..2).find! { |i| c.nodes[c.address(i)] != leader }
      original = c.address(index)
      copy_dir = c.new_dir
      FileUtils.cp(File.join(c.dirs[index], ".raft_state"), copy_dir)
      _, copy = c.start_extra(index + 1, [c.address(0), c.address(1), c.address(2)], copy_dir)
      sleep 1.second
      membership = leader.membership.should_not(be_nil)
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

  it "recognizes a connection to itself under another address" do
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    alias_addr = "localhost:#{server.local_address.port}"
    events = Channel(Raft::TransportEvent).new(16)
    transport = Raft::TCPTransport.new("secret", 1, addr, [alias_addr], ->(e : Raft::TransportEvent) { events.send e })
    spawn transport.listen(server)
    events.receive.should eq Raft::Identified.new(alias_addr, 1)
    transport.probe(alias_addr).should eq 1
  ensure
    transport.try &.close
  end

  it "delivers frames that shrink and grow over one connection intact" do
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    events = Channel(Raft::TransportEvent).new(16)
    peer = Raft::TCPTransport.new("secret", 1, addr, Array(String).new, ->(e : Raft::TransportEvent) { events.send e })
    spawn peer.listen(server)
    client = Raft::TCPTransport.new("secret", 2, "client:1", [addr], ->(_e : Raft::TransportEvent) { })
    events.receive.should eq Raft::Connected.new(2, "client:1")
    # Larger than the initial read buffer, with addresses that differ per size
    big = ->(n : Int32) do
      addresses = (1..n).to_h { |i| {i, "node-#{n}-#{i}.example.com:5680"} }
      membership = Raft::Membership.new(addresses.keys.to_set, Set(Int32).new, addresses)
      Raft::InstallSnapshot.new(2, 3, "tcp://client:5679", 9, 2, Set{1, 2}, membership).as(Raft::Message)
    end
    sent = [big.call(300), Raft::TimeoutNow.new(2, 3), big.call(200), big.call(400), Raft::AppendResponse.new(2, 3, true, 9)]
    sent.each { |msg| client.send(addr, msg) }
    sent.each do |msg|
      select
      when event = events.receive
        event.should eq msg
      when timeout(3.seconds)
        fail "didn't receive #{msg.class}"
      end
    end
  ensure
    client.try &.close
    peer.try &.close
  end

  # Something at a peer's address that doesn't know the password, e.g. after
  # the peer went down, mustn't be taken for it
  it "refuses a peer that accepts it without knowing the password" do
    rogue = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{rogue.local_address.port}"
    spawn do
      while socket = rogue.accept?
        socket.write Bytes.new(Raft::TCPTransport::NONCE_SIZE)
        # MAGIC, nonce, id, address, MAC
        socket.read_fully(Bytes.new(Raft::TCPTransport::MAGIC.size + Raft::TCPTransport::NONCE_SIZE + 4))
        socket.read_fully(Bytes.new(socket.read_bytes(Int32, Raft::Codec::Format) + Raft::TCPTransport::MAC_SIZE))
        socket.write_byte 1u8 # Ok
        socket.write_bytes 99, Raft::Codec::Format
        socket.write Bytes.new(Raft::TCPTransport::MAC_SIZE)
        socket.flush
      end
    end
    client = Raft::TCPTransport.new("secret", 2, "client:1", Array(String).new, ->(_e : Raft::TransportEvent) { })
    client.probe(addr).should be_nil
  ensure
    client.try &.close
    rogue.try &.close
  end

  # Frames are decoded straight from the socket, leftovers would be read as
  # the start of the next one
  it "drops a connection with a frame longer than its message" do
    server = TCPServer.new("127.0.0.1", 0)
    addr = "127.0.0.1:#{server.local_address.port}"
    events = Channel(Raft::TransportEvent).new(16)
    peer = Raft::TCPTransport.new("secret", 1, addr, Array(String).new, ->(e : Raft::TransportEvent) { events.send e })
    spawn peer.listen(server)
    socket = TCPSocket.new("127.0.0.1", server.local_address.port)
    nonce = Bytes.new(Raft::TCPTransport::NONCE_SIZE)
    socket.read_fully(nonce)
    client_nonce = Bytes.new(Raft::TCPTransport::NONCE_SIZE, 7u8)
    signed = IO::Memory.new
    signed.write "client".to_slice
    signed.write nonce
    signed.write client_nonce
    signed.write_bytes 2, Raft::Codec::Format
    signed.write "raw:1".to_slice
    socket.write Raft::TCPTransport::MAGIC
    socket.write client_nonce
    socket.write_bytes 2, Raft::Codec::Format
    Raft::Codec.write_str socket, "raw:1"
    socket.write OpenSSL::HMAC.digest(:sha256, "secret", signed.to_slice)
    socket.read_byte.should eq 1 # accepted
    socket.read_bytes Int32, Raft::Codec::Format
    socket.read_fully(Bytes.new(Raft::TCPTransport::MAC_SIZE))
    events.receive.should eq Raft::Connected.new(2, "raw:1")
    msg = Raft::Codec.encode(Raft::TimeoutNow.new(2, 3))
    socket.write_bytes (msg.size + 3).to_u32, Raft::Codec::Format
    socket.write msg
    socket.write Bytes.new(3)
    socket.flush
    select
    when event = events.receive
      event.should eq Raft::Disconnected.new(2, "raw:1")
    when timeout(3.seconds)
      fail "kept reading after a frame with bytes left over"
    end
  ensure
    socket.try &.close
    peer.try &.close
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
      loaded = storage.load.should_not(be_nil)
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
      Raft::SeedInfo.new(3, 0, 0, has_data: true, prior_in_sync: false),
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
