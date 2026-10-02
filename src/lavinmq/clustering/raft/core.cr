require "./messages"

module LavinMQ::Clustering::Raft
  enum Role
    Follower
    Candidate
    Leader
  end

  record IdConflict, addr : String, holder : String, node_id : Int32 do
    def to_s(io : IO) : Nil
      io << addr << " and " << holder << " both have clustering id " << node_id.to_s(36)
    end
  end

  # What must be on disk before any message produced alongside it is sent.
  record HardState, term : Int64, voted_for : String?,
    snapshot_index : Int64, snapshot_term : Int64, snapshot_isr : Set(Int32)?,
    entries : Array(Entry), peer_node_ids = Hash(String, Int32).new,
    snapshot_membership : Membership? = nil

  enum TransferResult
    # TimeoutNow was sent to the target
    Sent
    # The target is behind, TimeoutNow is sent when it has caught up
    Pending
    NotLeader
    # Not a voter in the ISR that we know the id of
    NotEligible
  end

  enum MembershipError
    NotLeader
    # The leader hasn't committed an entry in its term yet
    NotServing
    # Another membership change isn't committed yet
    Pending
    AlreadyMember
    NotLearner
    UnknownMember
    IsLeader
    NotInIsr
    NotCaughtUp
    # Leadership was lost before the change was committed, it may still be
    # committed by the next leader
    Lost

    def message : String
      case self
      in NotLeader     then "Not the leader"
      in NotServing    then "The leader hasn't committed an entry in its term yet"
      in Pending       then "Another membership change is pending"
      in AlreadyMember then "Already a member"
      in NotLearner    then "Not a learner"
      in UnknownMember then "Not a member"
      in IsLeader      then "The leader can't be removed, transfer leadership first"
      in NotInIsr      then "Not in the in-sync replica set yet"
      in NotCaughtUp   then "Not caught up with the leader's log yet"
      in Lost          then "Leadership was lost before the change was committed"
      end
    end
  end

  # One single-server membership change, see Core#propose_membership.
  enum MembershipChange
    AddLearner
    Promote
    Remove
  end

  # Raft (leader election + log replication) over a state machine holding
  # only the ISR. Pure: no IO, fibers or clocks, time is passed in. The
  # owner feeds it messages and ticks, then must persist `hard_state` when
  # `dirty?` and only after that send the `outbox`.
  #
  # Deviations from textbook Raft, all to fit LavinMQ:
  # - A vote (and pre-vote) is only granted to a candidate listed in the
  #   voter's latest ISR, so a node lacking confirmed messages can never
  #   lead. Any committed ISR is on a majority, which every winner needs
  #   votes from.
  # - A node that has never been part of the cluster (empty log) doesn't
  #   campaign unless `bootstrap` is set: nothing tells it whether its data
  #   is current, e.g. when starting a new cluster or after migrating from
  #   etcd.
  # - Membership changes one server at a time (Raft dissertation §4.1): add as
  #   learner (not counted), promote (one more voter), remove. The latest
  #   membership entry in the log is in effect as soon as it's appended, so
  #   truncating it reverts it, and the leader only has one uncommitted change
  #   at a time, and only once its term's no-op is committed. Removing a node
  #   also takes it out of the ISR in the same entry, so a removed node that
  #   missed the entry still can't win a vote.
  # - Pre-vote, so a node rejoining after a partition doesn't inflate the
  #   term and depose a healthy leader.
  # - Leader stickiness: votes are refused while a leader was heard from
  #   within the minimum election timeout, and a leader steps down when it
  #   hasn't heard from a majority for that long (check-quorum). Together
  #   they bound how long a deposed leader can believe it still leads.
  class Core
    getter id : String
    getter node_id : Int32
    getter term = 0i64
    getter voted_for : String? = nil
    getter role = Role::Follower
    getter leader : String? = nil
    getter leader_uri : String? = nil
    getter commit_index = 0i64
    getter outbox = Array(Tuple(String, Message)).new
    getter? dirty = false
    # Set when two raft addresses claim the same clustering id, e.g. after a
    # data dir was copied. ISR eligibility is by id, so the address that
    # claimed it last is never followed, voted for or counted, and when the id
    # is this node's own it doesn't campaign either. Cleared once one of them
    # reports another id.
    getter id_conflict : IdConflict? = nil

    @snapshot_index = 0i64
    @snapshot_term = 0i64
    @snapshot_isr : Set(Int32)? = nil
    @snapshot_membership : Membership? = nil
    @entries = Array(Entry).new
    # The configured peers, used until the log has a membership
    @seed_peers : Array(String)
    # Everyone but ourselves that the latest membership lists, voters and learners
    @peers = Array(String).new
    @voters = Set(String).new
    @transfer_target : Tuple(String, Time::Instant)? = nil
    # Nodes this leader removed that haven't acked the removal yet, with its
    # index and when to give up. Without that they'd never find out and keep
    # waiting for a leader.
    @departing = Hash(String, Tuple(Int64, Time::Instant)).new
    @now : Time::Instant
    @votes = Set(String).new
    @pre_votes = Set(String).new
    @pre_voting = false
    @next_index = Hash(String, Int64).new
    @match_index = Hash(String, Int64).new
    @last_ack = Hash(String, Time::Instant).new
    @peer_node_ids = Hash(String, Int32).new
    @term_start_index = 0i64
    @election_deadline : Time::Instant
    @heartbeat_due : Time::Instant
    @last_heard_leader : Time::Instant? = nil

    def initialize(@id : String, peers : Enumerable(String), @node_id : Int32, @uri : String,
                   @election_timeout : Time::Span, @heartbeat_interval : Time::Span,
                   now : Time::Instant, state : HardState? = nil, @random : Random = Random.new,
                   @bootstrap = false)
      @seed_peers = peers.reject(@id).uniq!
      @now = now
      if state
        @term = state.term
        @voted_for = state.voted_for
        @snapshot_index = state.snapshot_index
        @snapshot_term = state.snapshot_term
        @snapshot_isr = state.snapshot_isr
        @snapshot_membership = state.snapshot_membership
        @entries = state.entries.dup
        @peer_node_ids = state.peer_node_ids.dup
        @commit_index = @snapshot_index
      end
      @election_deadline = now + randomized_election_timeout
      @heartbeat_due = now
      refresh_membership
    end

    def hard_state : HardState
      HardState.new(@term, @voted_for, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup, @peer_node_ids.dup,
        @snapshot_membership)
    end

    def persisted : Nil
      @dirty = false
    end

    def take_outbox : Array(Tuple(String, Message))
      msgs = @outbox
      @outbox = Array(Tuple(String, Message)).new
      msgs
    end

    def last_index : Int64
      @snapshot_index + @entries.size
    end

    def last_term : Int64
      @entries.last?.try(&.term) || @snapshot_term
    end

    # The ISR of the latest entry in the log, committed or not.
    def latest_isr : Set(Int32)?
      @entries.reverse_each { |e| e.isr.try { |isr| return isr } }
      @snapshot_isr
    end

    def committed_isr : Set(Int32)?
      (@commit_index - @snapshot_index).to_i.downto(1) do |i|
        @entries[i - 1].isr.try { |isr| return isr }
      end
      @snapshot_isr
    end

    # The membership of the latest entry in the log, committed or not. It's
    # nil until a leader has seeded it from its configured peers.
    def latest_membership : Membership?
      @entries.reverse_each { |e| e.membership.try { |m| return m } }
      @snapshot_membership
    end

    def committed_membership : Membership?
      @snapshot_membership
    end

    # The clustering ids peers have reported, by raft address. Don't mutate.
    def peer_node_ids : Hash(String, Int32)
      @peer_node_ids
    end

    # Everyone but ourselves in the latest membership, learners included
    def peers : Array(String)
      @peers
    end

    # Removed nodes that are still being told so
    def departing : Array(String)
      @departing.keys
    end

    def match_index(addr : String) : Int64
      @match_index[addr]? || 0i64
    end

    def voter?(addr : String) : Bool
      @voters.includes?(addr)
    end

    # Followers that lag at most this far behind the leader's log count as
    # caught up.
    def caught_up?(addr : String) : Bool
      @role.leader? && match_index(addr) >= last_index
    end

    # Leader whose no-op of this term is committed, i.e. it has applied every
    # entry committed by earlier leaders and may act on the ISR.
    def serving_leader? : Bool
      @role.leader? && @commit_index >= @term_start_index
    end

    def quorum : Int32
      @voters.size // 2 + 1
    end

    # Append an ISR change. Returns its index, or nil when not the leader.
    # It's committed once `commit_index` reaches the index while still leader
    # in the same term. Nodes that were removed from the cluster stay out.
    def propose(isr : Set(Int32), now : Time::Instant) : Int64?
      return unless @role.leader?
      @now = now
      members = latest_membership.try(&.members)
      isr = isr.reject { |id| (addr = @peer_node_ids.key_for?(id)) && members && !members.includes?(addr) && addr != @id }.to_set
      append Entry.new(@term, isr)
      advance_commit
      broadcast_append
      last_index
    end

    # Append a single-server membership change. Returns its index, or why it
    # was refused. It's committed once `commit_index` reaches the index while
    # still leader in the same term. Only one change can be in flight, and only
    # once the leader has committed an entry in its term, otherwise two leaders
    # could each add a server and form disjoint majorities.
    # ameba:disable Metrics/CyclomaticComplexity
    def propose_membership(change : MembershipChange, addr : String, now : Time::Instant) : Int64 | MembershipError
      return MembershipError::NotLeader unless @role.leader?
      @now = now
      return MembershipError::NotServing unless serving_leader?
      return MembershipError::Pending if @entries.any?(&.membership)
      current = latest_membership || return MembershipError::NotServing
      voters = current.voters.dup
      learners = current.learners.dup
      isr = nil
      case change
      in .add_learner?
        return MembershipError::AlreadyMember if current.includes?(addr)
        learners << addr
      in .promote?
        return MembershipError::NotLearner unless learners.includes?(addr)
        node_id = @peer_node_ids[addr]?
        return MembershipError::NotInIsr unless node_id && committed_isr.try(&.includes?(node_id))
        return MembershipError::NotCaughtUp unless caught_up?(addr)
        learners.delete(addr)
        voters << addr
      in .remove?
        return MembershipError::IsLeader if addr == @id
        return MembershipError::UnknownMember unless current.includes?(addr)
        voters.delete(addr)
        learners.delete(addr)
        if (node_id = @peer_node_ids[addr]?) && (latest = latest_isr)
          isr = latest.dup.tap &.delete(node_id)
        end
      end
      next_index = @next_index[addr]?
      match_index = @match_index[addr]?
      append Entry.new(@term, isr, Membership.new(voters, learners))
      if change.remove?
        @departing[addr] = {last_index, now + @election_timeout * 5}
        @next_index[addr] = next_index || last_index
        @match_index[addr] = match_index || 0i64
      end
      advance_commit
      broadcast_append
      last_index
    end

    def tick(now : Time::Instant) : Nil
      @now = now
      if (t = @transfer_target) && now >= t[1]
        @transfer_target = nil
      end
      expire_departing(now) unless @departing.empty?
      if @role.leader?
        if lost_quorum?(now)
          become_follower(@term, nil)
          return
        end
        if now >= @heartbeat_due
          @heartbeat_due = now + @heartbeat_interval
          broadcast_append
        end
      elsif now >= @election_deadline
        @election_deadline = now + randomized_election_timeout
        start_pre_vote(now)
      end
    end

    # Hand leadership to `target`, a voter in the ISR, or without one to any
    # fully caught up such peer. A target that's behind gets TimeoutNow as soon
    # as it has caught up, but no later than an election timeout from now.
    def transfer_leadership(target : String? = nil) : TransferResult
      return TransferResult::NotLeader unless @role.leader?
      if target
        return TransferResult::NotEligible unless transfer_eligible?(target)
        if @match_index[target]? == last_index
          @transfer_target = nil
          send target, TimeoutNow.new(@id, @term)
          return TransferResult::Sent
        end
        @transfer_target = {target, @now + @election_timeout}
        send_append(target)
        TransferResult::Pending
      else
        peer = @peers.find { |p| transfer_eligible?(p) && @match_index[p]? == last_index }
        return TransferResult::NotEligible unless peer
        send peer, TimeoutNow.new(@id, @term)
        TransferResult::Sent
      end
    end

    private def transfer_eligible?(peer : String) : Bool
      return false unless @voters.includes?(peer) && @peers.includes?(peer)
      return false unless node_id = @peer_node_ids[peer]?
      isr = latest_isr
      !isr.nil? && isr.includes?(node_id)
    end

    def step(msg : Message, now : Time::Instant) : Nil
      @now = now
      case msg
      in RequestVote     then handle_request_vote(msg, now)
      in VoteResponse    then handle_vote_response(msg, now)
      in AppendEntries   then handle_append_entries(msg, now)
      in AppendResponse  then handle_append_response(msg, now)
      in InstallSnapshot then handle_install_snapshot(msg, now)
      in TimeoutNow      then handle_timeout_now(msg, now)
      in CatchUp         then handle_catch_up(msg, now)
      end
    end

    private def handle_request_vote(msg : RequestVote, now : Time::Instant) : Nil
      unless claim_node_id(msg.from, msg.node_id)
        send msg.from, VoteResponse.new(@id, msg.pre_vote ? msg.term : @term, false, pre_vote: msg.pre_vote)
        return
      end
      up_to_date = log_up_to_date?(msg.last_log_index, msg.last_log_term)
      candidate_in_isr = in_isr?(latest_isr, msg.node_id)
      eligible = up_to_date && candidate_in_isr
      sticky = !msg.transfer && leader_recent?(now)
      if msg.pre_vote
        granted = msg.term > @term && !sticky && eligible
        send msg.from, VoteResponse.new(@id, msg.term, granted, pre_vote: true)
      else
        handle_vote(msg, eligible, sticky, now)
      end
      if candidate_in_isr && !up_to_date
        send msg.from, CatchUp.new(@id, @term, @node_id, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup,
          @snapshot_membership)
      end
    end

    private def handle_vote(msg : RequestVote, eligible : Bool, sticky : Bool, now : Time::Instant) : Nil
      if msg.term > @term
        if sticky
          send msg.from, VoteResponse.new(@id, @term, false, pre_vote: false)
          return
        end
        become_follower(msg.term, nil)
      end
      granted = msg.term == @term && eligible && (@voted_for.nil? || @voted_for == msg.from)
      if granted && @voted_for != msg.from
        @voted_for = msg.from
        @dirty = true
      end
      @election_deadline = now + randomized_election_timeout if granted
      send msg.from, VoteResponse.new(@id, @term, granted, pre_vote: false)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def handle_vote_response(msg : VoteResponse, now : Time::Instant) : Nil
      if msg.pre_vote
        return unless @pre_voting && msg.granted && msg.term == @term + 1
        return unless @voters.includes?(msg.from)
        @pre_votes << msg.from
        start_election(now, transfer: false) if @pre_votes.size >= quorum
        return
      end
      if msg.term > @term
        become_follower(msg.term, nil)
        return
      end
      return unless @role.candidate? && msg.term == @term && msg.granted
      return unless @voters.includes?(msg.from)
      @votes << msg.from
      become_leader(now) if @votes.size >= quorum
    end

    private def handle_append_entries(msg : AppendEntries, now : Time::Instant) : Nil
      return unless claim_node_id(msg.from, msg.node_id)
      if msg.term < @term
        send msg.from, AppendResponse.new(@id, @term, @node_id, false, last_index)
        return
      end
      accept_leader(msg.term, msg.from, msg.leader_uri, now)
      if msg.prev_index > last_index
        send msg.from, AppendResponse.new(@id, @term, @node_id, false, last_index)
        return
      end
      if msg.prev_index >= @snapshot_index && term_at(msg.prev_index) != msg.prev_term
        # Conflicting entries are never committed, drop them
        truncate_from(msg.prev_index)
        send msg.from, AppendResponse.new(@id, @term, @node_id, false, msg.prev_index - 1)
        return
      end
      merge_entries(msg.prev_index, msg.entries)
      match = msg.prev_index + msg.entries.size
      if msg.commit > @commit_index
        commit_to Math.min(msg.commit, match)
      end
      send msg.from, AppendResponse.new(@id, @term, @node_id, true, match)
    end

    private def handle_install_snapshot(msg : InstallSnapshot, now : Time::Instant) : Nil
      return unless claim_node_id(msg.from, msg.node_id)
      if msg.term < @term
        send msg.from, AppendResponse.new(@id, @term, @node_id, false, last_index)
        return
      end
      accept_leader(msg.term, msg.from, msg.leader_uri, now)
      install_snapshot(msg.index, msg.snapshot_term, msg.isr, msg.membership)
      send msg.from, AppendResponse.new(@id, @term, @node_id, true, msg.index)
    end

    # Adopts a voter's log when it's more up to date than ours. That never
    # drops a committed entry: a log with a later last term holds every entry
    # committed before that term, one with the same last term extends ours.
    private def handle_catch_up(msg : CatchUp, now : Time::Instant) : Nil
      return unless claim_node_id(msg.from, msg.node_id)
      become_follower(msg.term, nil) if msg.term > @term
      return if leader_recent?(now)
      last = msg.snapshot_index + msg.entries.size
      last_term = msg.entries.last?.try(&.term) || msg.snapshot_term
      return unless last_term > self.last_term || (last_term == self.last_term && last > last_index)
      install_snapshot(msg.snapshot_index, msg.snapshot_term, msg.snapshot_isr, msg.snapshot_membership)
      merge_entries(msg.snapshot_index, msg.entries)
    end

    private def install_snapshot(index : Int64, term : Int64, isr : Set(Int32)?, membership : Membership?) : Nil
      return if index <= @commit_index
      @entries.clear
      @snapshot_index = index
      @snapshot_term = term
      @snapshot_isr = isr
      @snapshot_membership = membership
      @commit_index = index
      @dirty = true
      refresh_membership
    end

    private def merge_entries(prev_index : Int64, entries : Array(Entry)) : Nil
      entries.each_with_index do |entry, i|
        index = prev_index + 1 + i
        next if index <= @snapshot_index
        if index <= last_index
          next if term_at(index) == entry.term
          truncate_from(index)
        end
        append entry
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def handle_append_response(msg : AppendResponse, now : Time::Instant) : Nil
      if msg.term > @term
        become_follower(msg.term, nil)
        return
      end
      return unless @role.leader? && msg.term == @term
      return unless claim_node_id(msg.from, msg.node_id)
      @last_ack[msg.from] = now
      if msg.success
        if msg.match_index > (@match_index[msg.from]? || 0i64)
          @match_index[msg.from] = msg.match_index
          advance_commit
        end
        @next_index[msg.from] = Math.max(@next_index[msg.from]? || 1i64, msg.match_index + 1)
        send_append(msg.from) if @next_index[msg.from] <= last_index
        if (d = @departing[msg.from]?) && msg.match_index >= d[0]
          @departing.delete(msg.from)
          refresh_membership
        end
        if (t = @transfer_target) && t[0] == msg.from && msg.match_index == last_index
          @transfer_target = nil
          send msg.from, TimeoutNow.new(@id, @term)
        end
      else
        current = @next_index[msg.from]? || last_index + 1
        @next_index[msg.from] = Math.max(1i64, Math.min(current - 1, msg.match_index + 1))
        send_append(msg.from)
      end
    end

    private def handle_timeout_now(msg : TimeoutNow, now : Time::Instant) : Nil
      return unless msg.term == @term && msg.from == @leader
      return unless may_campaign?
      start_election(now, transfer: true)
    end

    private def accept_leader(term : Int64, leader : String, uri : String, now : Time::Instant) : Nil
      if term > @term || !@role.follower?
        become_follower(term, leader)
      end
      @pre_voting = false
      @leader = leader
      @leader_uri = uri
      @last_heard_leader = now
      @election_deadline = now + randomized_election_timeout
    end

    private def may_campaign? : Bool
      return false if @id_conflict.try(&.holder) == @id
      return false unless @voters.includes?(@id)
      return false unless in_isr?(latest_isr, @node_id)
      @bootstrap || last_index > 0
    end

    private def start_pre_vote(now : Time::Instant) : Nil
      return unless may_campaign?
      @pre_voting = true
      @pre_votes.clear
      @pre_votes << @id
      if @pre_votes.size >= quorum
        start_election(now, transfer: false)
        return
      end
      voting_peers.each do |p|
        send p, RequestVote.new(@id, @term + 1, @node_id, last_index, last_term, pre_vote: true, transfer: false)
      end
    end

    private def start_election(now : Time::Instant, transfer : Bool) : Nil
      @pre_voting = false
      @role = Role::Candidate
      @term += 1
      @voted_for = @id
      @leader = nil
      @leader_uri = nil
      @dirty = true
      @votes.clear
      @votes << @id
      @election_deadline = now + randomized_election_timeout
      if @votes.size >= quorum
        become_leader(now)
        return
      end
      voting_peers.each do |p|
        send p, RequestVote.new(@id, @term, @node_id, last_index, last_term, pre_vote: false, transfer: transfer)
      end
    end

    private def become_follower(term : Int64, leader : String?) : Nil
      if term > @term
        @term = term
        @voted_for = nil
        @dirty = true
      end
      @role = Role::Follower
      @pre_voting = false
      @transfer_target = nil
      @departing.clear
      @leader = leader
      @leader_uri = nil if leader.nil?
    end

    private def become_leader(now : Time::Instant) : Nil
      @role = Role::Leader
      @leader = @id
      @leader_uri = @uri
      @transfer_target = nil
      @departing.clear
      @peers.each do |p|
        @next_index[p] = last_index + 1
        @match_index[p] = 0i64
        @last_ack[p] = now
      end
      # Seed what a previous leader hasn't, like the first leader's ISR
      append Entry.new(@term, latest_isr ? nil : Set{@node_id}, latest_membership ? nil : seed_membership)
      @term_start_index = last_index
      advance_commit
      @heartbeat_due = now + @heartbeat_interval
      broadcast_append
    end

    private def lost_quorum?(now : Time::Instant) : Bool
      acked = self_vote
      voting_peers.each do |p|
        if last = @last_ack[p]?
          acked += 1 if now - last < @election_timeout
        end
      end
      acked < quorum
    end

    private def leader_recent?(now : Time::Instant) : Bool
      return true if @role.leader?
      return false unless @leader
      if heard = @last_heard_leader
        now - heard < @election_timeout
      else
        false
      end
    end

    # Remembers the clustering id a peer reported. Returns false, and the
    # message must be ignored, when another address already holds that id.
    private def claim_node_id(addr : String, node_id : Int32) : Bool
      holder = node_id == @node_id ? @id : @peer_node_ids.key_for?(node_id)
      if holder && holder != addr
        @id_conflict = IdConflict.new(addr, holder, node_id)
        return false
      end
      if (c = @id_conflict) && addr.in?(c.addr, c.holder) && node_id != c.node_id
        @id_conflict = nil
      end
      if @peer_node_ids[addr]? != node_id
        @peer_node_ids[addr] = node_id
        @dirty = true
      end
      true
    end

    private def in_isr?(isr : Set(Int32)?, node_id : Int32) : Bool
      isr.nil? || isr.includes?(node_id)
    end

    private def log_up_to_date?(index : Int64, term : Int64) : Bool
      term > last_term || (term == last_term && index >= last_index)
    end

    private def broadcast_append : Nil
      @peers.each { |p| send_append(p) }
      @departing.each_key { |p| send_append(p) }
    end

    private def expire_departing(now : Time::Instant) : Nil
      expired = @departing.select { |_, d| now >= d[1] }.keys
      return if expired.empty?
      expired.each { |addr| @departing.delete(addr) }
      refresh_membership
    end

    private def tracked?(addr : String) : Bool
      @peers.includes?(addr) || @departing.has_key?(addr)
    end

    private def voting_peers : Array(String)
      @peers.select { |p| @voters.includes?(p) }
    end

    # Our own vote, if we're a voter
    private def self_vote : Int32
      @voters.includes?(@id) ? 1 : 0
    end

    private def seed_membership : Membership
      Membership.new((@seed_peers + [@id]).to_set, Set(String).new)
    end

    # Recompute who we talk to and who counts after the log or snapshot changed.
    # Without a membership in the log the configured peers are all voters.
    private def refresh_membership : Nil
      if m = latest_membership
        @voters = m.voters.dup
        @peers = m.members.reject(@id)
      else
        @voters = (@seed_peers + [@id]).to_set
        @peers = @seed_peers.dup
      end
      # Forget what we knew about peers that left, they may come back as new
      @next_index.reject! { |p, _| !tracked?(p) }
      @match_index.reject! { |p, _| !tracked?(p) }
      @last_ack.reject! { |p, _| !tracked?(p) }
      @peers.each do |p|
        next if @next_index.has_key?(p)
        @next_index[p] = last_index + 1
        @match_index[p] = 0i64
        @last_ack[p] = @now
      end
    end

    private def send_append(peer : String) : Nil
      next_index = @next_index[peer]? || last_index + 1
      if next_index <= @snapshot_index
        send peer, InstallSnapshot.new(@id, @term, @node_id, @uri, @snapshot_index, @snapshot_term, @snapshot_isr,
          @snapshot_membership)
        return
      end
      prev = next_index - 1
      entries = @entries[(next_index - @snapshot_index - 1).to_i..]? || Array(Entry).new
      send peer, AppendEntries.new(@id, @term, @node_id, @uri, prev, term_at(prev), entries, @commit_index)
    end

    private def advance_commit : Nil
      n = last_index
      while n > @commit_index
        break if term_at(n) != @term # only entries of the current term are committed by counting
        replicated = self_vote + voting_peers.count { |p| (@match_index[p]? || 0i64) >= n }
        if replicated >= quorum
          commit_to n
          return
        end
        n -= 1
      end
    end

    # Committed entries are folded into the snapshot right away: the state is
    # a single ISR, so there's nothing to gain from keeping them.
    private def commit_to(index : Int64) : Nil
      return if index <= @commit_index
      isr = @snapshot_isr
      membership = @snapshot_membership
      count = (index - @snapshot_index).to_i
      @entries.first(count).each do |e|
        e.isr.try { |s| isr = s }
        e.membership.try { |m| membership = m }
      end
      @snapshot_term = term_at(index)
      @snapshot_isr = isr
      @snapshot_membership = membership
      @entries.shift(count)
      @snapshot_index = index
      @commit_index = index
      @dirty = true
    end

    private def term_at(index : Int64) : Int64
      return @snapshot_term if index == @snapshot_index
      return 0i64 if index < @snapshot_index
      @entries[(index - @snapshot_index - 1).to_i]?.try(&.term) || 0i64
    end

    private def append(entry : Entry) : Nil
      @entries << entry
      @dirty = true
      refresh_membership if entry.membership
    end

    private def truncate_from(index : Int64) : Nil
      keep = (index - @snapshot_index - 1).to_i
      return if keep >= @entries.size || keep < 0
      reverted = @entries[keep..].any?(&.membership)
      @entries.truncate(0, keep)
      @dirty = true
      refresh_membership if reverted
    end

    private def send(to : String, msg : Message) : Nil
      @outbox << {to, msg}
    end

    private def randomized_election_timeout : Time::Span
      @election_timeout + @election_timeout * @random.rand
    end
  end
end
