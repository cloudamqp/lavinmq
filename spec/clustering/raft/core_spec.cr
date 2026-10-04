require "spec"
require "../../../src/lavinmq/clustering/raft/core"

private alias Raft = LavinMQ::Clustering::Raft

# Deterministic network of Cores: a fake clock, delivery in send order, and
# nodes that can be cut off or crashed. A crashed node restarts from its
# last persisted HardState, like the real Node does. Node n has clustering
# id n and address "nN", and every running node is connected to every other
# one, like through the transport.
private class SimCluster
  ELECTION  = 100.milliseconds
  HEARTBEAT = 20.milliseconds

  getter now : Time::Instant = Time.instant
  getter cores = Hash(Int32, Raft::Core).new
  getter isolated = Set(Int32).new
  getter crashed = Set(Int32).new
  @disk = Hash(Int32, Raft::HardState?).new
  @addresses = Hash(Int32, String).new
  @seeds : Array(String)
  @inflight = Deque(Tuple(Int32, Raft::Message)).new

  def initialize(size : Int32, seed = 1, @bootstrap : Array(Int32)? = nil)
    (1..size).each { |id| @addresses[id] = "n#{id}" }
    @seeds = @addresses.values
    @addresses.each_key do |id|
      @disk[id] = nil
      @cores[id] = new_core(id, seed)
    end
    @cores.each_key { |id| connect(id) }
  end

  def [](id : Int32) : Raft::Core
    @cores[id]
  end

  def leaders : Array(Raft::Core)
    @cores.values.select { |c| c.role.leader? && !@crashed.includes?(c.id) }
  end

  def leader : Raft::Core?
    leaders.max_by?(&.term)
  end

  def crash(id : Int32)
    @crashed << id
  end

  def restart(id : Int32, seed = 7)
    @crashed.delete(id)
    @cores[id] = new_core(id, seed, @disk[id])
    connect(id)
  end

  # Restarts a node at another address, with its data dir
  def move(id : Int32, address : String, seed = 7)
    @addresses[id] = address
    restart(id, seed)
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

  def change(core : Raft::Core, change : Raft::MembershipChange, id : Int32,
             address : String = "n#{id}") : Int64 | Raft::MembershipError
    result = core.propose_membership(change, id, @now, change.add_learner? ? address : nil)
    pump
    result
  end

  # A node that starts with itself and existing members as peers, like one
  # being added to a running cluster.
  def join(id : Int32, seeds : Array(Int32), seed = 11, state : Raft::HardState? = nil,
           address = "n#{id}") : Raft::Core
    @addresses[id] = address
    @disk[id] = state
    @cores[id] = Raft::Core.new(id, address, [address] + seeds.map { |s| @addresses[s] }, "tcp://#{address}:5679",
      ELECTION, HEARTBEAT, @now, state, Random.new(seed), bootstrap: false)
    connect(id)
    @cores[id]
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

  private def connect(id : Int32)
    core = @cores[id]
    @cores.each do |other_id, other|
      next if other_id == id || @crashed.includes?(other_id)
      other.connected(id, @addresses[id], @now)
      core.connected(other_id, @addresses[other_id], @now)
    end
    pump
  end

  private def new_core(id, seed, state = nil)
    address = @addresses[id]
    Raft::Core.new(id, address, @seeds, "tcp://#{address}:5679", ELECTION, HEARTBEAT,
      @now, state, Random.new(seed + id - 1), bootstrap: @bootstrap.nil? || @bootstrap.not_nil!.includes?(id))
  end
end

# Whether the core starts a (pre-)vote once its election timeout has passed.
private def campaigns?(core : Raft::Core, now : Time::Instant) : Bool
  core.take_outbox
  core.tick(now + 1.second)
  core.take_outbox.any?(&.[1].is_a?(Raft::RequestVote))
end

private def three_voters : Raft::Membership
  Raft::Membership.new(Set{1, 2, 3}, Set(Int32).new, {1 => "n1", 2 => "n2", 3 => "n3"})
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
      c.leader_uri.should eq "tcp://n#{leader.id}:5679"
    end
  end

  it "elects itself in a single node cluster" do
    sim = SimCluster.new(1)
    sim.run_until { sim.leader.try &.serving_leader? }
  end

  it "only lets a bootstrap node campaign while no node has raft state" do
    sim = SimCluster.new(3, bootstrap: [2])
    sim.crash(2)
    sim.advance(2.seconds)
    sim.leader.should be_nil
    sim.restart(2)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should eq 2
  end

  it "lets a node without bootstrap campaign once it has joined" do
    sim = SimCluster.new(3, bootstrap: [1])
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.propose(sim.leader.not_nil!, Set{1, 2, 3})
    sim.advance(100.milliseconds)
    sim.crash(1)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.leader.not_nil!.id.should_not eq 1
  end

  it "doesn't let unbootstrapped nodes elect each other after hearing from the first leader" do
    sim = SimCluster.new(3, bootstrap: [1])
    sim.run_until { sim[2].last_index > 0 && sim[3].last_index > 0 }
    sim[2].latest_isr.should eq Set{1}
    sim.crash(1)
    sim.advance(2.seconds)
    sim.leader.should be_nil
  end

  it "fails over to an ISR member that missed an entry committed through a non-ISR node" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    a = sim.leader.not_nil!
    b, c = sim.cores.keys.reject(a.id)
    isr = Set{a.id, b}
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
    sim.cores.each_key { |id| sim.isolated << id unless id == leader.id }
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
    in_sync, lagging = sim.cores.keys.reject(leader.id)
    sim.propose(leader, Set{leader.id, in_sync})
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
    follower = sim.cores.keys.find! { |id| id != leader.id }
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
    sim.cores.values.each do |c|
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
    core = Raft::Core.new(1, "n1", ["n1", "n2"], "u1", 100.milliseconds, 20.milliseconds, Time.instant,
      Raft::HardState.new(5, nil, 0, 0, nil, [] of Raft::Entry))
    core.step(Raft::AppendEntries.new(2, 4, "u2", 0, 0, [Raft::Entry.new(4, Set{2})], 1), Time.instant)
    core.latest_isr.should be_nil
    _, reply = core.take_outbox.first
    reply.as(Raft::AppendResponse).success.should be_false
    reply.as(Raft::AppendResponse).term.should eq 5
  end

  it "refuses votes to a candidate with an older log" do
    now = Time.instant
    core = Raft::Core.new(1, "n1", ["n1", "n2", "n3"], "u1", 100.milliseconds, 20.milliseconds, now,
      Raft::HardState.new(3, nil, 0, 0, nil, [Raft::Entry.new(3, nil)]))
    core.connected(2, "n2", now)
    core.step(Raft::RequestVote.new(2, 4, 5, 2, pre_vote: false, transfer: false), now)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_false
  end

  it "only votes for a candidate connected from the address the membership lists for it" do
    now = Time.instant
    state = Raft::HardState.new(1, nil, 1, 1, nil, [] of Raft::Entry, three_voters)
    core = Raft::Core.new(1, "n1", ["n1", "n2", "n3"], "u1", 100.milliseconds, 20.milliseconds, now, state)
    core.connected(2, "n2b", now)
    core.step(Raft::RequestVote.new(2, 2, 1, 1, pre_vote: false, transfer: true), now)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_false
    core.disconnected(2, "n2b")
    core.connected(2, "n2", now)
    core.step(Raft::RequestVote.new(2, 2, 1, 1, pre_vote: false, transfer: true), now)
    _, reply = core.take_outbox.first
    reply.as(Raft::VoteResponse).granted.should be_true
  end

  it "doesn't campaign from another address than the membership lists for it" do
    now = Time.instant
    state = Raft::HardState.new(1, nil, 1, 1, nil, [] of Raft::Entry, three_voters)
    moved = Raft::Core.new(2, "n2b", ["n1", "n2", "n3"], "u2", 100.milliseconds, 20.milliseconds, now, state)
    campaigns?(moved, now).should be_false
    home = Raft::Core.new(2, "n2", ["n1", "n2", "n3"], "u2", 100.milliseconds, 20.milliseconds, now, state)
    campaigns?(home, now).should be_true
  end

  it "doesn't count acks from a voter connected from another address" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    x, y = sim.cores.keys.reject(leader.id)
    sim.isolated << x
    sim.isolated << y
    # Pending, so the leader doesn't relocate x yet
    index = sim.change(leader, Raft::MembershipChange::AddLearner, 4).as(Int64)
    # Like a copy of x's data dir running elsewhere
    leader.connected(x, "n9", sim.now)
    sim.isolated.delete(x)
    sim.advance(100.milliseconds)
    sim[x].last_index.should be >= index
    leader.commit_index.should be < index
  end

  it "hands leadership over to a caught up in-sync peer" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    sim.propose(old, sim.cores.keys.to_set)
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
    core = Raft::Core.new(1, "n1", peers, "tcp://n1:5679", SimCluster::ELECTION, SimCluster::HEARTBEAT,
      now, nil, Random.new(1), bootstrap: true)
    (2..5).each { |i| core.connected(i, "n#{i}", now) }
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new(2, 1, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new(3, 1, true, pre_vote: true), now)
    core.role.candidate?.should be_true
    core.term.should eq 1
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new(2, 2, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new(3, 1, true, pre_vote: false), now)
    core.role.leader?.should be_false
  end

  it "survives randomized crashes, partitions and moves with a single leader per term" do
    rng = Random.new(42)
    sim = SimCluster.new(5)
    leaders_by_term = Hash(Int64, Int32).new
    committed = Hash(Int64, Set(Int32)?).new
    moves = 0
    400.times do
      id = sim.cores.keys.sample(rng)
      down = sim.crashed.size + sim.isolated.size
      case rng.rand(5)
      when 0
        if sim.crashed.includes?(id)
          sim.restart(id, rng.rand(1000))
        elsif down < 2
          sim.crash(id)
        end
      when 1
        if sim.isolated.includes?(id)
          sim.isolated.delete(id)
        elsif down < 2
          sim.isolated << id
        end
      when 2
        if l = sim.leader
          reachable = sim.cores.keys.reject { |a| sim.crashed.includes?(a) || sim.isolated.includes?(a) }
          sim.propose(l, reachable.to_set << l.id)
        end
      when 3
        unless sim.crashed.includes?(id) || down >= 2
          sim.move(id, sim[id].address == "n#{id}" ? "n#{id}b" : "n#{id}", rng.rand(1000))
          moves += 1
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
    moves.should be > 5
  end
end

describe Raft::Core, "membership" do
  it "seeds the membership from the configured peers when the first leader is elected" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    sim.advance(100.milliseconds)
    sim.cores.each_value { |c| c.committed_membership.should eq three_voters }
  end

  it "seeds the membership only once it knows the clustering id of every configured peer" do
    now = Time.instant
    core = Raft::Core.new(1, "n1", ["n1", "n2", "n3"], "u1", SimCluster::ELECTION, SimCluster::HEARTBEAT,
      now, nil, Random.new(1), bootstrap: true)
    core.connected(2, "n2", now)
    core.quorum.should eq 2
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new(2, 1, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new(2, 1, true, pre_vote: false), now)
    core.role.leader?.should be_true
    core.step(Raft::AppendResponse.new(2, 1, true, core.last_index), now)
    core.serving_leader?.should be_true
    core.latest_membership.should be_nil
    core.connect_to.should eq Set{"n2", "n3"}
    core.identified("n3", 3)
    core.latest_membership.should eq three_voters
  end

  it "replicates to a learner without counting it towards commit or quorum" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    sim.join(4, [leader.id])
    index = sim.change(leader, Raft::MembershipChange::AddLearner, 4).as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    sim[4].latest_membership.not_nil!.learners.should eq Set{4}
    leader.quorum.should eq 2

    # With both voters cut off the learner has the entry but it can't commit
    sim.cores.each_key { |id| sim.isolated << id if id.in?(1, 2, 3) && id != leader.id }
    pending = sim.propose(leader, Set{1, 2, 3, 4}).not_nil!
    sim.advance(50.milliseconds)
    sim[4].last_index.should be >= pending
    leader.commit_index.should be < pending
  end

  it "never lets a learner campaign" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    sim.join(4, [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, 4)
    sim.advance(200.milliseconds)
    sim[4].last_index.should be > 0
    campaigns?(sim[4], sim.now).should be_false
  end

  it "raises the quorum when a learner is promoted" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.join(4, [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, 4)
    sim.advance(200.milliseconds)
    leader.quorum.should eq 2
    sim.propose(leader, Set{1, 2, 3, 4})
    sim.advance(100.milliseconds)
    index = sim.change(leader, Raft::MembershipChange::Promote, 4).as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    leader.quorum.should eq 3
    sim[4].voter?(4).should be_true
  end

  it "only promotes a learner that is in the ISR and has caught up" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.join(4, [leader.id])
    sim.change(leader, Raft::MembershipChange::AddLearner, 4)
    sim.advance(200.milliseconds)
    sim.change(leader, Raft::MembershipChange::Promote, 4).should eq Raft::MembershipError::NotInIsr
    sim.propose(leader, Set{1, 2, 3, 4})
    sim.advance(200.milliseconds)
    sim.isolated << 4
    sim.propose(leader, Set{1, 2, 3, 4})
    sim.change(leader, Raft::MembershipChange::Promote, 4).should eq Raft::MembershipError::NotCaughtUp
    sim.isolated.delete(4)
    sim.advance(100.milliseconds)
    sim.change(leader, Raft::MembershipChange::Promote, 4).should be_a Int64
  end

  it "rejects a second change while the first is pending" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.cores.each_key { |id| sim.isolated << id unless id == leader.id }
    first = sim.change(leader, Raft::MembershipChange::AddLearner, 4).as(Int64)
    leader.commit_index.should be < first
    sim.change(leader, Raft::MembershipChange::AddLearner, 5).should eq Raft::MembershipError::Pending
    sim.isolated.clear
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= first
    sim.change(leader, Raft::MembershipChange::AddLearner, 5).should be_a Int64
  end

  it "rejects adding a learner at a member's address" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    sim.change(leader, Raft::MembershipChange::AddLearner, 4, "n2").should eq Raft::MembershipError::AddressInUse
    sim.change(leader, Raft::MembershipChange::AddLearner, 2, "n4").should eq Raft::MembershipError::AlreadyMember
  end

  it "doesn't let a new leader change the membership before its no-op is committed" do
    now = Time.instant
    core = Raft::Core.new(1, "n1", ["n1", "n2", "n3"], "u1", SimCluster::ELECTION, SimCluster::HEARTBEAT,
      now, nil, Random.new(1), bootstrap: true)
    core.connected(2, "n2", now)
    core.connected(3, "n3", now)
    now += 1.second
    core.tick(now)
    core.step(Raft::VoteResponse.new(2, 1, true, pre_vote: true), now)
    core.step(Raft::VoteResponse.new(2, 1, true, pre_vote: false), now)
    core.role.leader?.should be_true
    core.propose_membership(Raft::MembershipChange::AddLearner, 4, now, "n4").should eq Raft::MembershipError::NotServing
    core.step(Raft::AppendResponse.new(2, 1, true, core.last_index), now)
    core.serving_leader?.should be_true
    core.propose_membership(Raft::MembershipChange::AddLearner, 4, now, "n4").should be_a Int64
  end

  it "reverts an uncommitted membership entry that is truncated after a leader crash" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    sim.cores.each_key { |id| sim.isolated << id unless id == old.id }
    sim.change(old, Raft::MembershipChange::AddLearner, 4).should be_a Int64
    old.latest_membership.not_nil!.learners.should eq Set{4}
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
    n4 = sim.join(4, [leader.id])
    n4.latest_membership.should be_nil
    campaigns?(n4, sim.now).should be_false
    sim.change(leader, Raft::MembershipChange::AddLearner, 4)
    sim.advance(200.milliseconds)
    n4.latest_membership.not_nil!.voters.should eq Set{1, 2, 3}
    n4.latest_membership.not_nil!.learners.should eq Set{4}
    n4.voter?(4).should be_false
    n4.leader.should eq leader.id
    campaigns?(n4, sim.now).should be_false
  end

  it "takes the removed node out of the ISR in the same entry" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |id| id != leader.id }
    index = sim.change(leader, Raft::MembershipChange::Remove, removed).as(Int64)
    sim.advance(200.milliseconds)
    leader.commit_index.should be >= index
    leader.committed_isr.should eq Set{1, 2, 3} - Set{removed}
    leader.committed_membership.not_nil!.members.should_not contain(removed)
    leader.committed_membership.not_nil!.addresses.has_key?(removed).should be_false
    leader.quorum.should eq 2
  end

  it "doesn't let a removed node with a stale configuration win an election" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |id| id != leader.id }
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
    removed = sim.cores.keys.find! { |id| id != leader.id }
    sim.isolated << removed
    sim.change(leader, Raft::MembershipChange::Remove, removed)
    sim.advance(100.milliseconds)
    sim[removed].latest_membership.not_nil!.includes?(removed).should be_true
    leader.departing.should eq [removed]
    leader.connect_to.should contain("n#{removed}")
    sim.isolated.delete(removed)
    sim.advance(100.milliseconds)
    sim[removed].latest_membership.not_nil!.includes?(removed).should be_false
    leader.departing.should be_empty
    leader.connect_to.should_not contain("n#{removed}")
    campaigns?(sim[removed], sim.now).should be_false
  end

  it "stops telling a removed node that doesn't answer" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |id| id != leader.id }
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
    sim.change(leader, Raft::MembershipChange::Remove, 9).should eq Raft::MembershipError::UnknownMember
  end

  it "keeps nodes that aren't members out of the ISR" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3, 4})
    leader.committed_isr.should eq Set{1, 2, 3}
  end

  it "keeps a removed node out of later ISR updates" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    removed = sim.cores.keys.find! { |id| id != leader.id }
    sim.change(leader, Raft::MembershipChange::Remove, removed)
    sim.advance(200.milliseconds)
    sim.propose(leader, Set{1, 2, 3})
    sim.advance(100.milliseconds)
    leader.committed_isr.should eq Set{1, 2, 3} - Set{removed}
  end

  it "makes a voter that moved a learner at its new address, then promotes it back" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    moved = sim.cores.keys.find! { |id| id != leader.id }
    sim.move(moved, "n#{moved}b")
    sim.run_until { leader.committed_membership.not_nil!.addresses[moved] == "n#{moved}b" }
    membership = leader.committed_membership.not_nil!
    membership.learners.should eq Set{moved}
    membership.relocated.should eq Set{moved}
    leader.quorum.should eq 2
    sim.run_until { leader.committed_membership.not_nil!.voters.includes?(moved) }
    leader.committed_membership.not_nil!.relocated.should be_empty
    leader.quorum.should eq 2
    sim.advance(100.milliseconds)
    sim[moved].committed_membership.not_nil!.addresses[moved].should eq "n#{moved}b"
    sim[moved].voter?(moved).should be_true
  end

  it "promotes a relocated node back only once it's in the ISR" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    moved, other = sim.cores.keys.reject(leader.id)
    sim.propose(leader, Set{leader.id, other})
    sim.advance(100.milliseconds)
    sim.move(moved, "n#{moved}b")
    sim.advance(500.milliseconds)
    leader.committed_membership.not_nil!.learners.should eq Set{moved}
    sim.propose(leader, Set{1, 2, 3})
    sim.run_until { leader.committed_membership.not_nil!.voters.includes?(moved) }
  end

  it "takes a removed node back at a new address with the same data dir" do
    sim = SimCluster.new(3)
    leader = sim.elect(Set{1, 2, 3})
    moved = sim.cores.keys.find! { |id| id != leader.id }
    sim.change(leader, Raft::MembershipChange::Remove, moved)
    sim.advance(200.milliseconds)
    leader.departing.should be_empty
    sim.crash(moved)
    sim.change(leader, Raft::MembershipChange::AddLearner, moved, "n9").should be_a Int64
    sim.advance(100.milliseconds)
    sim.move(moved, "n9")
    sim.advance(200.milliseconds)
    sim.propose(leader, Set{1, 2, 3})
    sim.advance(100.milliseconds)
    leader.committed_isr.not_nil!.should contain(moved)
    sim.change(leader, Raft::MembershipChange::Promote, moved).should be_a Int64
  end
end

describe Raft::Core, "leadership transfer" do
  it "hands leadership to the chosen target" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    target = sim.cores.keys.reverse!.find! { |id| id != old.id }
    old.transfer_leadership(target).should eq Raft::TransferResult::Sent
    sim.pump
    sim.advance(50.milliseconds)
    sim.leader.not_nil!.id.should eq target
  end

  it "sends TimeoutNow to a lagging target once it has caught up" do
    sim = SimCluster.new(3)
    old = sim.elect(Set{1, 2, 3})
    target = sim.cores.keys.find! { |id| id != old.id }
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
    target = sim.cores.keys.find! { |id| id != old.id }
    sim.isolated << target
    sim.propose(old, Set{1, 2, 3})
    old.transfer_leadership(target).should eq Raft::TransferResult::Pending
    sim.advance(SimCluster::ELECTION * 2)
    sim.isolated.delete(target)
    sim.advance(300.milliseconds)
    sim.leader.should be old
  end

  it "refuses a target outside the ISR, a learner and an unknown node" do
    sim = SimCluster.new(3)
    sim.run_until { sim.leader.try &.serving_leader? }
    old = sim.leader.not_nil!
    others = sim.cores.keys.reject(old.id)
    not_in_isr = others.last
    sim.propose(old, Set{old.id, others.first})
    sim.advance(100.milliseconds)
    old.transfer_leadership(not_in_isr).should eq Raft::TransferResult::NotEligible
    old.transfer_leadership(9).should eq Raft::TransferResult::NotEligible
    old.transfer_leadership(old.id).should eq Raft::TransferResult::NotEligible

    sim.propose(old, Set{old.id, others.first, 4})
    sim.join(4, [old.id])
    sim.change(old, Raft::MembershipChange::AddLearner, 4)
    sim.advance(200.milliseconds)
    old.transfer_leadership(4).should eq Raft::TransferResult::NotEligible
    sim[others.first].transfer_leadership(others.last).should eq Raft::TransferResult::NotLeader
  end
end
