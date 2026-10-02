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

  def start(addr : String, index = @addrs.index!(addr))
    server = @servers[addr]? || (@servers[addr] = TCPServer.new("127.0.0.1", addr.split(':').last.to_i))
    node = @nodes[addr] = Raft::Node.new(addr, @addrs, index + 1, "tcp://#{addr}", Raft::Storage.new(dirs[index]),
      100.milliseconds, 20.milliseconds, 5.milliseconds, bootstrap: true)
    transport = @transports[addr] = Raft::TCPTransport.new(@password, @addrs.reject(addr), ->node.deliver(Raft::Message))
    spawn transport.listen(server)
    node.run(transport)
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

  it "hands over leadership on transfer" do
    with_raft_cluster do |c|
      leader = c.wait_for_leader
      leader.propose_isr(Set{1, 2, 3}).should be_true
      wait_for { c.nodes.values.all? { |n| n.committed_isr == Set{1, 2, 3} } }
      leader.transfer_leadership.should be_true
      wait_for(1.second) { !leader.leader? }
      c.wait_for_leader(except: leader)
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
    node = Raft::Node.new("127.0.0.1:1", ["127.0.0.1:1"], 1, "tcp://127.0.0.1:1", Raft::Storage.new(dir),
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
      node = Raft::Node.new(addr, addrs, i + 1, "tcp://#{addr}", Raft::Storage.new(File.join(dir, i.to_s).tap { |d| Dir.mkdir_p d }),
        100.milliseconds, 20.milliseconds, 5.milliseconds, bootstrap: true)
      transport = Raft::TCPTransport.new("password#{i}", addrs.reject(addr), ->node.deliver(Raft::Message))
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
end

describe Raft::Storage do
  it "round-trips the hard state" do
    with_datadir do |dir|
      storage = Raft::Storage.new(dir)
      storage.load.should be_nil
      state = Raft::HardState.new(7, "n2", 3, 6, Set{1, 2}, [Raft::Entry.new(7, nil), Raft::Entry.new(7, Set{2})],
        {"n2" => 2, "n3" => 3})
      storage.save(state)
      storage.load.should eq state
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
      Raft::RequestVote.new("a", 3, 42, 9, 2, pre_vote: true, transfer: false),
      Raft::VoteResponse.new("b", 3, true, pre_vote: false),
      Raft::AppendEntries.new("a", 3, 2, "tcp://a:5679", 8, 2, [Raft::Entry.new(3, nil), Raft::Entry.new(3, Set{1, 42})], 7),
      Raft::AppendResponse.new("b", 3, 7, false, 5),
      Raft::InstallSnapshot.new("a", 3, 2, "tcp://a:5679", 8, 2, Set{42}),
      Raft::TimeoutNow.new("a", 3),
    ] of Raft::Message
    msgs.each { |m| Raft::Codec.decode(Raft::Codec.encode(m)).should eq m }
  end
end
