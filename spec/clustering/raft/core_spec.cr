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

  def change(core : Raft::Core, change : Raft::MembershipChange, addr : String) : Int64 | Raft::MembershipError
    result = core.propose_membership(change, addr, @now)
    pump
    result
  end

  # A node that starts with itself and an existing member as peers, like one
  # being added to a running cluster.
  def join(addr : String, seeds : Array(String), seed = 11) : Raft::Core
    @addrs << addr
    @disk[addr] = nil
    @cores[addr] = Raft::Core.new(addr, [addr] + seeds, @addrs.size, "tcp://#{addr}:5679", ELECTION, HEARTBEAT,
      @now, nil, Random.new(seed), bootstrap: false)
  end

  # Runs until there's a serving leader and has it commit `isr`.
  def elect(isr : Set(Int32)) : Raft::Core
    run_until { leader.try &.serving_leader? }
    l = leader.not_nil!
    propose(l, isr)
    advance(100.milliseconds)
    l
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
        @cores[to]?.try &.step(msg, @now)
      end
    end
  end

  private def new_core(addr, index, seed, state = nil)
    Raft::Core.new(addr, @addrs, index + 1, "tcp://#{addr}:5679", ELECTION, HEARTBEAT,
      @now, state, Random.new(seed + index), bootstrap: @bootstrap.nil? || @bootstrap.not_nil!.includes?(addr))
  end
end

# Whether the core starts a (pre-)vote once its election timeout has passed.
private def campaigns?(core : Raft::Core, now : Time::Instant) : Bool
  core.take_outbox
  core.tick(now + 1.second)
  core.take_outbox.any?(&.[1].is_a?(Raft::RequestVote))
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

  it "doesn't let unbootstrapped nodes elect each other after hearing from the first leader" do
    sim = SimCluster.new(3, bootstrap: ["n1"])
    sim.run_until { sim["n2"].last_index > 0 && sim["n3"].last_index > 0 }
    sim["n2"].latest_isr.should eq Set{1}
    sim.crash("n1")
    sim.advance(2.seconds)
    sim.leader.should be_nil
  end

  it "fails over to an ISR member that missed an entry committed through a non-ISR node" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    a = sim.leader.not_nil!
    b, c = sim.cores.keys.reject(a.id)
    isr = Set{sim.node_id(a.id), sim.node_id(b)}
    sim.propose(a, isr)
    sim.advance(100.milliseconds)
    sim.isolated << b
    index = sim.propose(a, isr).not_nil!
    a.commit_index.should be >= index
    sim[c].last_index.should be > sim[b].last_index
    sim.crash(a.id)
    sim.isolated.delete(b)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should eq b
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
    sim.propose(old, Set{1, 2, 3})
    sim.advance(100.milliseconds)
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
    core.step(Raft::AppendEntries.new("n2", 4, 2, "u2", 0, 0, [Raft::Entry.new(4, Set{2})], 1), Time.instant)
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

  it "refuses votes to a candidate claiming another node's clustering id" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now)
    core.step(Raft::AppendEntries.new("n2", 1, 2, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core.take_outbox
    now += 1.second
    core.step(Raft::RequestVote.new("n3", 2, 2, 0, 0, pre_vote: false, transfer: true), now)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_false
    core.id_conflict.should_not be_nil
  end

  it "campaigns again once the node with its clustering id gets a new one" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now, bootstrap: true)
    core.step(Raft::AppendEntries.new("n2", 1, 1, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core.id_conflict.should_not be_nil
    core.step(Raft::AppendEntries.new("n2", 1, 5, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core.id_conflict.should be_nil
    campaigns?(core, now).should be_true
  end

  it "keeps campaigning when two other peers share a clustering id" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now, bootstrap: true)
    core.step(Raft::AppendEntries.new("n2", 1, 2, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core.step(Raft::RequestVote.new("n3", 2, 2, 0, 0, pre_vote: true, transfer: false), now)
    core.id_conflict.should_not be_nil
    campaigns?(core, now).should be_true
  end

  it "remembers peer clustering ids across restarts" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now)
    core.step(Raft::AppendEntries.new("n2", 1, 2, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now, core.hard_state)
    core.step(Raft::RequestVote.new("n3", 2, 2, 0, 0, pre_vote: false, transfer: true), now)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_false
  end

  it "refuses to follow or campaign when a peer has its clustering id" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", 100.milliseconds, 20.milliseconds, now, bootstrap: true)
    core.step(Raft::AppendEntries.new("n2", 1, 1, "u2", 0, 0, [] of Raft::Entry, 0), now)
    core.leader.should be_nil
    core.take_outbox.should be_empty
    core.id_conflict.should_not be_nil
    campaigns?(core, now).should be_false
  end

  it "doesn't count a peer with a duplicate clustering id towards commit" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    leader = sim.leader.not_nil!
    others = sim.cores.keys.reject(leader.id)
    sim.isolated << others[0]
    index = leader.propose(Set{sim.node_id(leader.id)}, sim.now).not_nil!
    leader.step(Raft::AppendResponse.new(others[1], leader.term, sim.node_id(others[0]), true, index), sim.now)
    leader.step(Raft::AppendResponse.new("n9", leader.term, sim.node_id(leader.id), true, index), sim.now)
    leader.commit_index.should be < index
  end

  it "hands leadership over to a caught up in-sync peer" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    sim.propose(old, sim.cores.keys.map { |a| sim.node_id(a) }.to_set)
    sim.advance(100.milliseconds)
    old.transfer_leadership.should eq Raft::TransferResult::Sent
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

describe Raft::Core, "membership" do
  it "seeds the membership from the configured peers when the first leader is elected" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.advance(100.milliseconds)
    expected = Raft::Membership.new(Set{"n1", "n2", "n3"}, Set(String).new)
    sim.cores.each_value { |c| c.committed_membership.should eq expected }
  end

  it "replicates to a learner without counting it towards commit or quorum" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    sim.join("n4", [leader.id])
    index = sim.change(leader, Raft::MembershipChange::AddLearner, "n4").as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    sim["n4"].latest_membership.not_nil!.learners.should eq Set{"n4"}
    leader.quorum.should eq 2

    # With both voters cut off the learner has the entry but it can't commit
    sim.cores.each_key { |a| sim.isolated << a if a.in?("n1", "n2", "n3") && a != leader.id }
    pending = sim.propose(leader, Set{1, 2, 3, 4}).not_nil!
    sim.advance(50.milliseconds)
    sim["n4"].last_index.should be >= pending
    leader.commit_index.should be < pending
  end

  it "never lets a learner campaign" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    sim.join("n4", [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, "n4")
    sim.advance(200.milliseconds)
    sim["n4"].last_index.should be > 0
    campaigns?(sim["n4"], sim.now).should be_false
  end

  it "raises the quorum when a learner is promoted" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    sim.join("n4", [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, "n4")
    sim.advance(200.milliseconds)
    leader.quorum.should eq 2
    index = sim.change(leader, Raft::MembershipChange::Promote, "n4").as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    leader.quorum.should eq 3
    sim["n4"].voter?("n4").should be_true
  end

  it "only promotes a learner that is in the ISR and has caught up" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.join("n4", [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, "n4")
    sim.advance(200.milliseconds)
    sim.change(leader, Raft::MembershipChange::Promote, "n4").should eq Raft::MembershipError::NotInIsr
    sim.propose(leader, Set{1, 2, 3, 4})
    sim.advance(200.milliseconds)
    sim.isolated << "n4"
    sim.propose(leader, Set{1, 2, 3, 4})
    sim.change(leader, Raft::MembershipChange::Promote, "n4").should eq Raft::MembershipError::NotCaughtUp
    sim.isolated.delete("n4")
    sim.advance(100.milliseconds)
    sim.change(leader, Raft::MembershipChange::Promote, "n4").should be_a Int64
  end

  it "rejects a second change while the first is pending" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.cores.each_key { |a| sim.isolated << a unless a == leader.id }
    first = sim.change(leader, Raft::MembershipChange::AddLearner, "n4").as(Int64)
    leader.commit_index.should be < first
    sim.change(leader, Raft::MembershipChange::AddLearner, "n5").should eq Raft::MembershipError::Pending
    sim.isolated.clear
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= first
    sim.change(leader, Raft::MembershipChange::AddLearner, "n5").should be_a Int64
  end

  it "doesn't let a new leader change the membership before its no-op is committed" do
    now = Time.instant
    core = Raft::Core.new("n1", ["n1", "n2", "n3"], 1, "u1", SimCluster::ELECTION, SimCluster::HEARTBEAT,
      now, nil, Random.new(1), bootstrap: true)
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new("n2", 1, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new("n2", 1, true, pre_vote: false), now)
    core.role.leader?.should be_true
    core.propose_membership(Raft::MembershipChange::AddLearner, "n4", now).should eq Raft::MembershipError::NotServing
    core.step(Raft::AppendResponse.new("n2", 1, 2, true, core.last_index), now)
    core.serving_leader?.should be_true
    core.propose_membership(Raft::MembershipChange::AddLearner, "n4", now).should be_a Int64
  end

  it "reverts an uncommitted membership entry that is truncated after a leader crash" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    sim.cores.each_key { |a| sim.isolated << a unless a == old.id }
    sim.change(old, Raft::MembershipChange::AddLearner, "n4").should be_a Int64
    old.latest_membership.not_nil!.learners.should eq Set{"n4"}
    sim.isolated.clear
    sim.isolated << old.id
    sim.run_until { (l = sim.leader) && l.id != old.id && l.serving_leader? }
    sim.isolated.clear
    sim.advance(500.milliseconds)
    old.role.leader?.should be_false
    old.latest_membership.not_nil!.learners.should be_empty
    old.committed_membership.not_nil!.learners.should be_empty
    sim.leader.not_nil!.latest_membership.not_nil!.learners.should be_empty
  end

  it "adopts the membership from the snapshot when joining with an empty log" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    n4 = sim.join("n4", [leader.id])
    n4.latest_membership.should be_nil
    campaigns?(n4, sim.now).should be_false
    sim.change(leader, Raft::MembershipChange::AddLearner, "n4")
    sim.advance(200.milliseconds)
    n4.latest_membership.not_nil!.voters.should eq Set{"n1", "n2", "n3"}
    n4.latest_membership.not_nil!.learners.should eq Set{"n4"}
    n4.voter?("n4").should be_false
    n4.leader.should eq leader.id
    campaigns?(n4, sim.now).should be_false
  end

  it "takes the removed node out of the ISR in the same entry" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |a| a != leader.id }
    index = sim.change(leader, Raft::MembershipChange::Remove, removed).as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    leader.committed_isr.should eq Set{1, 2, 3} - Set{sim.node_id(removed)}
    leader.committed_membership.not_nil!.members.should_not contain(removed)
    leader.quorum.should eq 2
  end

  it "doesn't let a removed node with a stale configuration win an election" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |a| a != leader.id }
    sim.isolated << removed
    index = sim.change(leader, Raft::MembershipChange::Remove, removed).as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    sim[removed].latest_membership.not_nil!.voters.size.should eq 3
    sim.crash(leader.id)
    sim.isolated.delete(removed)
    sim.advance(3.seconds)
    sim[removed].role.leader?.should be_false
    sim.leader.should be_nil
  end

  it "tells a removed node that it was removed" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |a| a != leader.id }
    sim.isolated << removed
    sim.change(leader, Raft::MembershipChange::Remove, removed)
    sim.advance(100.milliseconds)
    sim[removed].latest_membership.not_nil!.includes?(removed).should be_true
    leader.departing.should eq [removed]
    sim.isolated.delete(removed)
    sim.advance(100.milliseconds)
    sim[removed].latest_membership.not_nil!.includes?(removed).should be_false
    leader.departing.should be_empty
    campaigns?(sim[removed], sim.now).should be_false
  end

  it "stops telling a removed node that doesn't answer" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |a| a != leader.id }
    sim.isolated << removed
    sim.change(leader, Raft::MembershipChange::Remove, removed)
    leader.departing.should eq [removed]
    sim.advance(SimCluster::ELECTION * 6)
    leader.departing.should be_empty
  end

  it "rejects removing the leader" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.change(leader, Raft::MembershipChange::Remove, leader.id).should eq Raft::MembershipError::IsLeader
    sim.change(leader, Raft::MembershipChange::Remove, "n9").should eq Raft::MembershipError::UnknownMember
  end

  it "keeps a removed node out of later ISR updates" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |a| a != leader.id }
    sim.change(leader, Raft::MembershipChange::Remove, removed)
    sim.advance(200.milliseconds)
    sim.propose(leader, Set{1, 2, 3})
    sim.advance(100.milliseconds)
    leader.committed_isr.should eq Set{1, 2, 3} - Set{sim.node_id(removed)}
  end
end

describe Raft::Core, "leadership transfer" do
  it "hands leadership to the chosen target" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    target = sim.cores.keys.reverse!.find! { |a| a != old.id }
    old.transfer_leadership(target).should eq Raft::TransferResult::Sent
    sim.pump
    sim.advance(50.milliseconds)
    sim.leader.not_nil!.id.should eq target
  end

  it "sends TimeoutNow to a lagging target once it has caught up" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    target = sim.cores.keys.find! { |a| a != old.id }
    sim.isolated << target
    sim.propose(old, Set{1, 2, 3})
    sim.advance(20.milliseconds)
    old.transfer_leadership(target).should eq Raft::TransferResult::Pending
    sim.isolated.delete(target)
    sim.advance(100.milliseconds)
    sim.leader.not_nil!.id.should eq target
  end

  it "gives up on a lagging target at the deadline" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    target = sim.cores.keys.find! { |a| a != old.id }
    sim.isolated << target
    sim.propose(old, Set{1, 2, 3})
    old.transfer_leadership(target).should eq Raft::TransferResult::Pending
    sim.advance(SimCluster::ELECTION * 2)
    sim.isolated.delete(target)
    sim.advance(300.milliseconds)
    sim.leader.should be old
  end

  it "refuses a target outside the ISR, a learner and an unknown address" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    others = sim.cores.keys.reject(old.id)
    not_in_isr = others.last
    sim.propose(old, Set{sim.node_id(old.id), sim.node_id(others.first)})
    sim.advance(100.milliseconds)
    old.transfer_leadership(not_in_isr).should eq Raft::TransferResult::NotEligible
    old.transfer_leadership("n9").should eq Raft::TransferResult::NotEligible
    old.transfer_leadership(old.id).should eq Raft::TransferResult::NotEligible

    sim.propose(old, Set{sim.node_id(old.id), sim.node_id(others.first), 4})
    sim.join("n4", [old.id])
    sim.change(old, Raft::MembershipChange::AddLearner, "n4")
    sim.advance(200.milliseconds)
    old.transfer_leadership("n4").should eq Raft::TransferResult::NotEligible
    sim[others.first].transfer_leadership(others.last).should eq Raft::TransferResult::NotLeader
  end
end
