require "spec"
require "../../../src/lavinmq/clustering/raft/core"

private alias Raft = LavinMQ::Clustering::Raft

# Deterministic network of Cores: a fake clock, delivery in send order, and
# nodes that can be cut off or crashed. A crashed node restarts from its
# last persisted HardState, like the real Node does.
private class SimCluster
  ELECTION  = 100.milliseconds
  HEARTBEAT = 20.milliseconds

  getter now : Time::Instant = Time.instant
  getter cores = Hash(String, Raft::Core).new
  getter isolated = Set(String).new
  getter crashed = Set(String).new
  @disk = Hash(String, Raft::HardState?).new
  @addrs : Array(String)
  @inflight = Deque(Tuple(String, Raft::Message)).new

  def initialize(size : Int32, seed = 1, @bootstrap : Array(String)? = nil)
    @addrs = (1..size).map { |i| "n#{i}" }
    @addrs.each_with_index do |a, i|
      @disk[a] = nil
      @cores[a] = new_core(a, i, seed)
    end
  end

  def node_id(addr : String) : Int32
    @addrs.index!(addr) + 1
  end

  def [](addr : String) : Raft::Core
    @cores[addr]
  end

  def leaders : Array(Raft::Core)
    @cores.values.select { |c| c.role.leader? && !@crashed.includes?(c.id) }
  end

  def leader : Raft::Core?
    leaders.max_by?(&.term)
  end

  def crash(addr : String)
    @crashed << addr
  end

  def restart(addr : String, seed = 7)
    @crashed.delete(addr)
    @cores[addr] = new_core(addr, @addrs.index!(addr), seed, @disk[addr])
  end

  def advance(span : Time::Span, step = 5.milliseconds)
    steps = (span / step).to_i
    steps.times do
      @now += step
      @cores.each_value { |c| c.tick(@now) unless @crashed.includes?(c.id) }
      pump
    end
  end

  def run_until(limit = 5.seconds, &)
    deadline = @now + limit
    until yield
      raise "condition not reached in #{limit}" if @now > deadline
      advance(5.milliseconds)
    end
  end

  def propose(core : Raft::Core, isr : Set(Int32)) : Int64?
    index = core.propose(isr, @now)
    pump
    index
  end

  def pump
    loop do
      @cores.each_value do |c|
        next if @crashed.includes?(c.id)
        if c.dirty?
          @disk[c.id] = c.hard_state
          c.persisted
        end
        c.take_outbox.each { |m| @inflight << m }
      end
      break if @inflight.empty?
      while item = @inflight.shift?
        to, msg = item
        next if @crashed.includes?(to) || @crashed.includes?(msg.from)
        next if @isolated.includes?(to) || @isolated.includes?(msg.from)
        @cores[to].step(msg, @now)
      end
    end
  end

  private def new_core(addr, index, seed, state = nil)
    Raft::Core.new(addr, @addrs, index + 1, "tcp://#{addr}:5679", ELECTION, HEARTBEAT,
      @now, state, Random.new(seed + index), bootstrap: @bootstrap.nil? || @bootstrap.not_nil!.includes?(addr))
  end
end

describe Raft::Core do
  it "elects exactly one leader" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader }
    sim.advance(1.second)
    sim.leaders.size.should eq 1
    leader = sim.leader.not_nil!
    sim.cores.each_value do |c|
      c.term.should eq leader.term
      c.leader_uri.should eq "tcp://#{leader.id}:5679"
    end
  end

  it "elects itself in a single node cluster" do
    sim = SimCluster.new(1)
    sim.run_until { sim.leader.try &.serving_leader? }
  end

  it "only lets a bootstrap node campaign while no node has raft state" do
    sim = SimCluster.new(3, bootstrap: ["n2"])
    sim.crash("n2")
    sim.advance(2.seconds)
    sim.leader.should be_nil
    sim.restart("n2")
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should eq "n2"
  end

  it "lets a node without bootstrap campaign once it has joined" do
    sim = SimCluster.new(3, bootstrap: ["n1"])
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.propose(sim.leader.not_nil!, Set{1, 2, 3})
    sim.advance(100.milliseconds)
    sim.crash("n1")
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should_not eq "n1"
  end

  it "commits an ISR change on a majority" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    leader = sim.leader.not_nil!
    index = sim.propose(leader, Set{1, 2, 3}).not_nil!
    leader.commit_index.should be >= index
    sim.advance(100.milliseconds)
    sim.cores.each_value(&.committed_isr.should(eq(Set{1, 2, 3})))
  end

  it "doesn't commit without a majority" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    leader = sim.leader.not_nil!
    sim.cores.each_key { |a| sim.isolated << a unless a == leader.id }
    index = sim.propose(leader, Set{1}).not_nil!
    sim.advance(50.milliseconds)
    leader.commit_index.should be < index
  end

  it "fails over when the leader crashes" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    sim.crash(old.id)
    sim.run_until { (l = sim.leader) && l.id != old.id && l.serving_leader? }
    sim.leader.not_nil!.term.should be > old.term
  end

  it "never elects a node outside the ISR" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    leader = sim.leader.not_nil!
    others = sim.cores.keys.reject(leader.id)
    in_sync, lagging = others
    sim.propose(leader, Set{sim.node_id(leader.id), sim.node_id(in_sync)})
    sim.advance(100.milliseconds)
    sim.crash(leader.id)
    sim.crash(in_sync)
    sim.advance(3.seconds)
    sim.leader.should be_nil
    sim[lagging].role.leader?.should be_false
    # The in-sync node comes back and can win together with the lagging vote
    sim.restart(in_sync)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should eq in_sync
  end

  it "lets a leader step down when it loses the majority" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    sim.isolated << old.id
    sim.advance(SimCluster::ELECTION * 2)
    old.role.leader?.should be_false
  end

  it "doesn't let a rejoining node depose a healthy leader" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    leader = sim.leader.not_nil!
    follower = sim.cores.keys.find! { |a| a != leader.id }
    sim.isolated << follower
    sim.advance(2.seconds) # pre-vote keeps the isolated node's term flat
    sim.isolated.delete(follower)
    sim.advance(500.milliseconds)
    sim.leader.should be leader
    sim[follower].term.should eq leader.term
  end

  it "doesn't vote twice in a term across restarts" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.cores.each_value do |c|
      next if c.role.leader?
      voted = c.voted_for
      term = c.term
      sim.crash(c.id)
      sim.restart(c.id)
      sim[c.id].term.should eq term
      sim[c.id].voted_for.should eq voted
    end
  end

  it "rejects stale-term append entries" do
    core = Raft::Core.new("n1", ["n1", "n2"], 1, "u1", 100.milliseconds, 20.milliseconds, Time.instant,
      Raft::HardState.new(5, nil, 0, 0, nil, [] of Raft::Entry))
    core.step(Raft::AppendEntries.new("n2", 4, "u2", 0, 0, [Raft::Entry.new(4, Set{2})], 1), Time.instant)
    core.latest_isr.should be_nil
    _, reply = core.take_outbox.first
    reply.as(Raft::AppendResponse).success.should be_false
    reply.as(Raft::AppendResponse).term.should eq 5
  end

  it "refuses votes to a candidate with an older log" do
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, Time.instant,
      Raft::HardState.new(3, nil, 0, 0, nil, [Raft::Entry.new(3, nil)]))
    core.step(Raft::RequestVote.new("n2", 4, 2, 5, 2, pre_vote: false, transfer: false), Time.instant)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_false
  end

  it "hands leadership over to a caught up in-sync peer" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    sim.propose(old, sim.cores.keys.map { |a| sim.node_id(a) }.to_set)
    sim.advance(100.milliseconds)
    old.transfer_leadership.should be_true
    sim.pump
    sim.advance(50.milliseconds) # well within the election timeout
    new_leader = sim.leader.not_nil!
    new_leader.id.should_not eq old.id
  end

  it "doesn't count pre-vote grants as votes in the current term" do
    peers = (1..5).map { |i| "n#{i}" }
    now = Time.instant
    core = Raft::Core.new("n1", peers, 1, "tcp://n1:5679", SimCluster::ELECTION, SimCluster::HEARTBEAT,
      now, nil, Random.new(1), bootstrap: true)
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new("n2", 1, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new("n3", 1, true, pre_vote: true), now)
    core.role.candidate?.should be_true
    core.term.should eq 1
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new("n2", 2, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new("n3", 1, true, pre_vote: false), now)
    core.role.leader?.should be_false
  end

  it "survives randomized crashes and partitions with a single leader per term" do
    rng = Random.new(42)
    sim = SimCluster.new(5)
    leaders_by_term = Hash(Int64, String).new
    committed = Hash(Int64, Set(Int32)?).new
    400.times do
      addr = sim.cores.keys.sample(rng)
      down = sim.crashed.size + sim.isolated.size
      case rng.rand(4)
      when 0
        if sim.crashed.includes?(addr)
          sim.restart(addr, rng.rand(1000))
        elsif down < 2
          sim.crash(addr)
        end
      when 1
        if sim.isolated.includes?(addr)
          sim.isolated.delete(addr)
        elsif down < 2
          sim.isolated << addr
        end
      when 2
        if l = sim.leader
          reachable = sim.cores.keys.reject { |a| sim.crashed.includes?(a) || sim.isolated.includes?(a) }
          sim.propose(l, reachable.map { |a| sim.node_id(a) }.to_set << sim.node_id(l.id))
        end
      end
      sim.advance(rng.rand(10..300).milliseconds)
      sim.cores.each_value do |c|
        next unless c.role.leader?
        if prev = leaders_by_term[c.term]?
          prev.should eq c.id
        end
        leaders_by_term[c.term] = c.id
      end
      # A committed index never changes meaning
      sim.cores.each_value do |c|
        next if sim.crashed.includes?(c.id)
        if prev = committed[c.commit_index]?
          c.committed_isr.should eq prev
        else
          committed[c.commit_index] = c.committed_isr
        end
      end
    end
    leaders_by_term.size.should be > 5
    committed.size.should be > 5
  end
end
