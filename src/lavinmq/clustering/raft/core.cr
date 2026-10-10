require "./messages"

module LavinMQ::Clustering::Raft
  enum Role
    Follower
    Candidate
    Leader
  end

  # What must be on disk before any message produced alongside it is sent.
  record HardState, term : Int64, voted_for : Int32?,
    snapshot_index : Int64, snapshot_term : Int64, snapshot_isr : Set(Int32)?,
    entries : Array(Entry), snapshot_membership : Membership? = nil

  enum TransferResult
    # TimeoutNow was sent to the target
    Sent
    # The target is behind, TimeoutNow is sent when it has caught up
    Pending
    NotLeader
    # Not a voter in the ISR, or not reachable where the membership says
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
    AddressInUse
    # The node couldn't be reached to learn its clustering id
    Unreachable
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
      in AddressInUse  then "Another member has that address"
      in Unreachable   then "Couldn't reach the node, start it first"
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
  # - Nodes are known by clustering id, their addresses are data in the
  #   membership. A voter only counts (acks, votes, candidacy) while it's
  #   connected from the address the membership lists for it. When it shows
  #   up elsewhere the leader makes it a learner at the new address, a
  #   single-server removal, and promotes it back once it's in the ISR and
  #   caught up. So a copied data dir running next to the original is never
  #   counted as the same voter twice. When most voters move at once no
  #   leader can do that, so after MOVED_TRUST_AFTER election timeouts
  #   without a leader the voters count each other wherever they are, and
  #   the leader elected records the new addresses. That trusts that a node
  #   at a new address is a move and not a copy running next to the original.
  # - Pre-vote, so a node rejoining after a partition doesn't inflate the
  #   term and depose a healthy leader.
  # - Leader stickiness: votes are refused while a leader was heard from
  #   within the minimum election timeout, and a leader steps down when it
  #   hasn't heard from a majority for that long (check-quorum). Together
  #   they bound how long a deposed leader can believe it still leads.
  class Core
    private record Departing, index : Int64, deadline : Time::Instant, address : String

    # Election timeouts without a leader before voters at new addresses are
    # trusted, see at_home?. Long enough that a normal election has had
    # every chance to finish first.
    MOVED_TRUST_AFTER = 3

    getter id : Int32
    getter address : String
    getter term = 0i64
    getter voted_for : Int32? = nil
    getter role = Role::Follower
    getter leader : Int32? = nil
    getter leader_uri : String? = nil
    @outbox = Array(Tuple(Int32, Message)).new
    getter? dirty = false

    @snapshot_index = 0i64
    @snapshot_term = 0i64
    @snapshot_isr : Set(Int32)? = nil
    @snapshot_membership : Membership? = nil
    @entries = Array(Entry).new
    # The configured seeds, used until the log has a membership
    @seed_addresses : Array(String)
    # The clustering ids of the configured seeds that we know of
    @seed_ids = Hash(String, Int32).new
    # Peers connected to us right now, with the address they advertise
    @live = Hash(Int32, String).new
    # Everyone but ourselves that the latest membership lists, voters and learners
    @peers = Array(Int32).new
    @voters = Set(Int32).new
    @transfer_target : Tuple(Int32, Time::Instant)? = nil
    # Nodes this leader removed that haven't acked the removal yet. Without
    # that they'd never find out and keep waiting for a leader.
    @departing = Hash(Int32, Departing).new
    @now : Time::Instant
    @votes = Set(Int32).new
    @pre_votes = Set(Int32).new
    @pre_voting = false
    @next_index = Hash(Int32, Int64).new
    @match_index = Hash(Int32, Int64).new
    @last_ack = Hash(Int32, Time::Instant).new
    # When each peer last answered this leader. Unlike @last_ack it isn't
    # seeded when becoming leader.
    @answered = Hash(Int32, Time::Instant).new
    @term_start_index = 0i64
    @election_deadline : Time::Instant
    @heartbeat_due : Time::Instant
    @last_heard_leader : Time::Instant? = nil
    # When this node last stopped having a leader: startup, or stepping
    # down. With @last_heard_leader it says how long we've been without one.
    @leaderless_since : Time::Instant
    # Set after MOVED_TRUST_AFTER election timeouts without a leader: voters
    # at other addresses than the membership lists are counted and a moved
    # node campaigns, see at_home?. Cleared on hearing from a leader, and by
    # a leader once it has recorded the new addresses.
    @trust_moved = false

    def initialize(@id : Int32, @address : String, seeds : Enumerable(String), @uri : String,
                   @election_timeout : Time::Span, @heartbeat_interval : Time::Span,
                   now : Time::Instant, state : HardState? = nil, @random : Random = Random.new,
                   @bootstrap = false)
      @seed_addresses = seeds.reject(@address).uniq!
      @now = now
      @leaderless_since = now
      if state
        @term = state.term
        @voted_for = state.voted_for
        @snapshot_index = state.snapshot_index
        @snapshot_term = state.snapshot_term
        @snapshot_isr = state.snapshot_isr
        @snapshot_membership = state.snapshot_membership
        @entries = state.entries.dup
      end
      @election_deadline = now + randomized_election_timeout
      @heartbeat_due = now
      refresh_membership
    end

    def hard_state : HardState
      HardState.new(@term, @voted_for, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup,
        @snapshot_membership)
    end

    def persisted : Nil
      @dirty = false
    end

    def take_outbox : Array(Tuple(Int32, Message))
      msgs = @outbox
      @outbox = Array(Tuple(Int32, Message)).new
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

    # Committed entries are folded into the snapshot right away, see
    # #commit_to, so the snapshot is everything that's committed
    def commit_index : Int64
      @snapshot_index
    end

    def committed_isr : Set(Int32)?
      @snapshot_isr
    end

    # The membership of the latest entry in the log, committed or not. It's
    # nil until a leader has made it from its seeds.
    def latest_membership : Membership?
      @entries.reverse_each { |e| e.membership.try { |m| return m } }
      @snapshot_membership
    end

    def committed_membership : Membership?
      @snapshot_membership
    end

    # Everyone but ourselves in the latest membership, learners included
    def peers : Array(Int32)
      @peers
    end

    def connected?(id : Int32) : Bool
      @live.has_key?(id)
    end

    # Removed nodes that are still being told so
    def departing : Array(Int32)
      @departing.keys
    end

    def match_index(id : Int32) : Int64
      @match_index[id]? || 0i64
    end

    def voter?(id : Int32) : Bool
      @voters.includes?(id)
    end

    # The address the membership lists for this voter when it has moved
    # since. It doesn't campaign until a leader has made it a learner at its
    # new address, which takes a quorum of voters at their listed addresses.
    def moved_from : String?
      return unless @voters.includes?(@id)
      listed = latest_membership.try(&.addresses[@id]?) || return
      listed unless listed == @address
    end

    # How long ago a follower last heard from the leader it knows of. A
    # follower keeps that leader while no new one is elected.
    def leader_heard_ago(now : Time::Instant) : Time::Span?
      return if @role.leader?
      @last_heard_leader.try { |heard| now - heard }
    end

    # How long this node has been without a leader: since it last heard from
    # one, started, or stepped down, whichever is latest.
    def leaderless_for(now : Time::Instant) : Time::Span
      return Time::Span.zero if @role.leader?
      now - leaderless_start
    end

    private def leaderless_start : Time::Instant
      since = @leaderless_since
      @last_heard_leader.try { |heard| since = heard if heard > since }
      since
    end

    # Whether voters at other addresses than the membership lists are being
    # counted, because there has been no leader for a long time.
    def trusting_moved? : Bool
      @trust_moved
    end

    # Whether a peer has answered this leader within the election timeout,
    # i.e. is up and reachable.
    def responsive?(id : Int32) : Bool
      return false unless @role.leader?
      answered = @answered[id]? || return false
      @now - answered < @election_timeout
    end

    # Followers that lag at most this far behind the leader's log count as
    # caught up.
    def caught_up?(id : Int32) : Bool
      @role.leader? && match_index(id) >= last_index
    end

    # Where to send to a node: where it's connected from, else where the
    # membership, a pending removal or the configured seeds say it is.
    def address_of(id : Int32) : String?
      @live[id]? || latest_membership.try(&.addresses[id]?) || @departing[id]?.try(&.address) ||
        @seed_ids.key_for?(id)
    end

    # The addresses to keep connections to: every member, the nodes being told
    # they were removed, the leader even if we don't know yet that it's a
    # member (a node joining with an empty log can only answer the leader that
    # way), and the configured seeds until there's a membership.
    def connect_to : Set(String)
      addrs = Set(String).new
      @peers.each { |p| address_of(p).try { |a| addrs << a } }
      @departing.each_value { |d| addrs << d.address }
      @leader.try { |l| address_of(l).try { |a| addrs << a } }
      @seed_addresses.each { |a| addrs << a } unless latest_membership
      addrs.delete(@address)
      addrs
    end

    # A peer connected to us, advertising `address`. Every message from it
    # arrives over such a connection, and only one address per id is
    # connected at a time.
    def connected(id : Int32, address : String, now : Time::Instant) : Nil
      @now = now
      return if id == @id
      @live[id] = address
      identified(address, id)
      manage_membership
    end

    def disconnected(id : Int32, address : String) : Nil
      @live.delete(id) if @live[id]? == address
    end

    # The node at `address` has clustering id `id`. Only matters for
    # seeds, until there's a membership.
    def identified(address : String, id : Int32) : Nil
      return if id == @id || !@seed_addresses.includes?(address) || @seed_ids[address]? == id
      @seed_ids[address] = id
      return if latest_membership
      refresh_membership
      manage_membership
    end

    # Leader whose no-op of this term is committed, i.e. it has applied every
    # entry committed by earlier leaders and may act on the ISR.
    def serving_leader? : Bool
      @role.leader? && @snapshot_index >= @term_start_index
    end

    # Before there's a membership every seed is a voter, also the
    # ones whose id we don't know yet and so can't count.
    def quorum : Int32
      voter_count = latest_membership ? @voters.size : @seed_addresses.size + 1
      voter_count // 2 + 1
    end

    # Append an ISR change. Returns its index, or nil when not the leader.
    # It's committed once `commit_index` reaches the index while still leader
    # in the same term. Nodes that were removed from the cluster stay out.
    def propose(isr : Set(Int32), now : Time::Instant) : Int64?
      return unless @role.leader?
      @now = now
      if m = latest_membership
        isr = isr.select { |id| m.includes?(id) }.to_set
      end
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
    # A learner is added with the address to reach it at.
    # ameba:disable Metrics/CyclomaticComplexity
    def propose_membership(change : MembershipChange, id : Int32, now : Time::Instant,
                           address : String? = nil) : Int64 | MembershipError
      return MembershipError::NotLeader unless @role.leader?
      @now = now
      return MembershipError::NotServing unless serving_leader?
      return MembershipError::Pending if membership_pending?
      current = latest_membership || return MembershipError::NotServing
      voters = current.voters.dup
      learners = current.learners.dup
      addresses = current.addresses.dup
      relocated = current.relocated.dup
      isr = nil
      departing_address = nil
      case change
      in .add_learner?
        address || raise ArgumentError.new("A learner needs an address")
        return MembershipError::AlreadyMember if current.includes?(id)
        return MembershipError::AddressInUse if addresses.values.includes?(address)
        learners << id
        addresses[id] = address
      in .promote?
        return MembershipError::NotLearner unless learners.includes?(id)
        return MembershipError::NotInIsr unless committed_isr.try(&.includes?(id))
        return MembershipError::NotCaughtUp unless caught_up?(id)
        learners.delete(id)
        relocated.delete(id)
        voters << id
      in .remove?
        return MembershipError::IsLeader if id == @id
        return MembershipError::UnknownMember unless current.includes?(id)
        departing_address = address_of(id)
        voters.delete(id)
        learners.delete(id)
        relocated.delete(id)
        addresses.delete(id)
        isr = latest_isr.try &.dup.tap &.delete(id)
      end
      next_index = @next_index[id]?
      match_index = @match_index[id]?
      append Entry.new(@term, isr, Membership.new(voters, learners, addresses, relocated))
      if departing_address
        @departing[id] = Departing.new(last_index, now + @election_timeout * 5, departing_address)
        @next_index[id] = next_index || last_index
        @match_index[id] = match_index || 0i64
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
        manage_membership
        if now >= @heartbeat_due
          @heartbeat_due = now + @heartbeat_interval
          broadcast_append
        end
      else
        @trust_moved = true if leaderless_for(now) >= @election_timeout * MOVED_TRUST_AFTER
        if now >= @election_deadline
          @election_deadline = now + randomized_election_timeout
          start_pre_vote(now)
        end
      end
    end

    # When #tick has something to do next. Nothing changes on its own before
    # then, so a caller can sleep until it unless a message or request comes.
    def next_deadline(now : Time::Instant) : Time::Instant
      if @role.leader?
        deadline = @heartbeat_due
        # Check-quorum: the earliest counted ack to expire, no later than
        # the quorum can be lost
        @peers.each do |p|
          next unless counted?(p)
          expires = (@last_ack[p]? || next) + @election_timeout
          deadline = expires if expires > now && expires < deadline
        end
      else
        deadline = @election_deadline
        unless @trust_moved
          deadline = {deadline, leaderless_start + @election_timeout * MOVED_TRUST_AFTER}.min
        end
      end
      @transfer_target.try { |t| deadline = {deadline, t[1]}.min }
      @departing.each_value { |d| deadline = {deadline, d.deadline}.min }
      deadline
    end

    # Hand leadership to `target`, a voter in the ISR, or without one to any
    # fully caught up such peer. A target that's behind gets TimeoutNow as soon
    # as it has caught up, but no later than an election timeout from now.
    def transfer_leadership(target : Int32? = nil) : TransferResult
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

    private def transfer_eligible?(peer : Int32) : Bool
      return false unless @voters.includes?(peer) && @peers.includes?(peer) && at_home?(peer) && responsive?(peer)
      isr = latest_isr
      !isr.nil? && isr.includes?(peer)
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
      up_to_date = log_up_to_date?(msg.last_log_index, msg.last_log_term)
      candidate_in_isr = in_isr?(latest_isr, msg.from)
      eligible = up_to_date && candidate_in_isr && @voters.includes?(msg.from) && at_home?(msg.from) &&
                 may_vote_for_log?(msg.last_log_index)
      sticky = !msg.transfer && leader_recent?(now)
      if msg.pre_vote
        granted = msg.term > @term && !sticky && eligible
        send msg.from, VoteResponse.new(@id, msg.term, granted, pre_vote: true)
      else
        handle_vote(msg, eligible, sticky, now)
      end
      if candidate_in_isr && !up_to_date
        send msg.from, CatchUp.new(@id, @term, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup,
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
        return unless @voters.includes?(msg.from) && at_home?(msg.from)
        @pre_votes << msg.from
        start_election(now, transfer: false) if @pre_votes.size >= quorum
        return
      end
      if msg.term > @term
        become_follower(msg.term, nil)
        return
      end
      return unless @role.candidate? && msg.term == @term && msg.granted
      return unless @voters.includes?(msg.from) && at_home?(msg.from)
      @votes << msg.from
      become_leader(now) if @votes.size >= quorum
    end

    private def handle_append_entries(msg : AppendEntries, now : Time::Instant) : Nil
      if msg.term < @term
        send msg.from, AppendResponse.new(@id, @term, false, last_index)
        return
      end
      accept_leader(msg.term, msg.from, msg.leader_uri, now)
      if msg.prev_index > last_index
        send msg.from, AppendResponse.new(@id, @term, false, last_index)
        return
      end
      if msg.prev_index >= @snapshot_index && term_at(msg.prev_index) != msg.prev_term
        # Conflicting entries are never committed, drop them
        truncate_from(msg.prev_index)
        send msg.from, AppendResponse.new(@id, @term, false, msg.prev_index - 1)
        return
      end
      merge_entries(msg.prev_index, msg.entries)
      match = msg.prev_index + msg.entries.size
      if msg.commit > @snapshot_index
        commit_to Math.min(msg.commit, match)
      end
      send msg.from, AppendResponse.new(@id, @term, true, match)
    end

    private def handle_install_snapshot(msg : InstallSnapshot, now : Time::Instant) : Nil
      if msg.term < @term
        send msg.from, AppendResponse.new(@id, @term, false, last_index)
        return
      end
      accept_leader(msg.term, msg.from, msg.leader_uri, now)
      install_snapshot(msg.index, msg.snapshot_term, msg.isr, msg.membership)
      send msg.from, AppendResponse.new(@id, @term, true, msg.index)
    end

    # Adopts a voter's log when it's more up to date than ours. That never
    # drops a committed entry: a log with a later last term holds every entry
    # committed before that term, one with the same last term extends ours.
    private def handle_catch_up(msg : CatchUp, now : Time::Instant) : Nil
      become_follower(msg.term, nil) if msg.term > @term
      return if leader_recent?(now)
      last = msg.snapshot_index + msg.entries.size
      last_term = msg.entries.last?.try(&.term) || msg.snapshot_term
      return unless last_term > self.last_term || (last_term == self.last_term && last > last_index)
      install_snapshot(msg.snapshot_index, msg.snapshot_term, msg.snapshot_isr, msg.snapshot_membership)
      merge_entries(msg.snapshot_index, msg.entries)
    end

    private def install_snapshot(index : Int64, term : Int64, isr : Set(Int32)?, membership : Membership?) : Nil
      return if index <= @snapshot_index
      @entries.clear
      @snapshot_index = index
      @snapshot_term = term
      @snapshot_isr = isr
      @snapshot_membership = membership
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
      # Its acks, also stale ones later, mustn't count as that member's
      return unless at_home?(msg.from)
      @last_ack[msg.from] = now
      @answered[msg.from] = now
      if msg.success
        if msg.match_index > (@match_index[msg.from]? || 0i64)
          @match_index[msg.from] = msg.match_index
          advance_commit
        end
        @next_index[msg.from] = Math.max(@next_index[msg.from]? || 1i64, msg.match_index + 1)
        send_append(msg.from) if @next_index[msg.from] <= last_index
        if (d = @departing[msg.from]?) && msg.match_index >= d.index
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

    private def accept_leader(term : Int64, leader : Int32, uri : String, now : Time::Instant) : Nil
      if term > @term || !@role.follower?
        become_follower(term, leader)
      end
      @pre_voting = false
      @leader = leader
      @leader_uri = uri
      @last_heard_leader = now
      @trust_moved = false
      @election_deadline = now + randomized_election_timeout
    end

    private def may_campaign? : Bool
      return false unless @voters.includes?(@id)
      # At a new address the leader first has to make us a learner here,
      # unless there has been no leader to do so for a long time
      return false if moved_from && !@trust_moved
      return false unless in_isr?(latest_isr, @id)
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
        send p, RequestVote.new(@id, @term + 1, last_index, last_term, pre_vote: true, transfer: false)
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
        send p, RequestVote.new(@id, @term, last_index, last_term, pre_vote: false, transfer: transfer)
      end
    end

    private def become_follower(term : Int64, leader : Int32?) : Nil
      if term > @term
        @term = term
        @voted_for = nil
        @dirty = true
      end
      @leaderless_since = @now if @role.leader?
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
      @answered.clear
      @peers.each do |p|
        @next_index[p] = last_index + 1
        @match_index[p] = 0i64
        @last_ack[p] = now
      end
      # Seed what a previous leader hasn't, like the first leader's ISR
      membership = if latest_membership.nil?
                     seed_membership if seeds_identified?
                   elsif @trust_moved
                     # Elected by counting voters at new addresses: record them
                     # in the first entry, so everyone is at home again and the
                     # normal rules apply from here on
                     moved_addresses_membership
                   end
      @trust_moved = false
      append Entry.new(@term, latest_isr ? nil : Set{@id}, membership)
      @term_start_index = last_index
      advance_commit
      @heartbeat_due = now + @heartbeat_interval
      broadcast_append
    end

    private def lost_quorum?(now : Time::Instant) : Bool
      acked = self_vote + @peers.count do |p|
        counted?(p) && (last = @last_ack[p]?) && now - last < @election_timeout
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

    # Leader housekeeping that is itself a membership change, so one at a
    # time: seed the membership once every seed's id is known,
    # make a member that connected from a new address a learner there, and
    # promote such a relocated member back once it's eligible.
    private def manage_membership : Nil
      return if !serving_leader? || membership_pending?
      unless m = latest_membership
        append_membership(seed_membership) if seeds_identified?
        return
      end
      @live.each do |id, address|
        next if !m.includes?(id) || m.addresses[id]? == address
        relocate(m, id, address)
        return
      end
      m.relocated.each do |id|
        next unless m.learners.includes?(id) && committed_isr.try(&.includes?(id)) && caught_up?(id)
        propose_membership(MembershipChange::Promote, id, @now)
        return
      end
    end

    # Demoting a voter is a single-server removal, so a majority of the old
    # voters always overlaps one of the new, even if the node at the old
    # address is still running from a copy of the data dir.
    private def relocate(m : Membership, id : Int32, address : String) : Nil
      voters = m.voters.dup
      learners = m.learners.dup
      relocated = m.relocated.dup
      if voters.delete(id)
        learners << id
        relocated << id
      end
      addresses = m.addresses.dup
      addresses[id] = address
      append_membership Membership.new(voters, learners, addresses, relocated)
    end

    # The membership with the current addresses of this node and of the
    # connected members, roles unchanged. Nil when nothing has moved.
    private def moved_addresses_membership : Membership?
      m = latest_membership || return
      addresses = m.addresses.dup
      addresses[@id] = @address if m.includes?(@id)
      @live.each { |id, address| addresses[id] = address if m.includes?(id) }
      return if addresses == m.addresses
      Membership.new(m.voters, m.learners, addresses, m.relocated)
    end

    private def append_membership(membership : Membership) : Nil
      append Entry.new(@term, nil, membership)
      advance_commit
      broadcast_append
    end

    private def membership_pending? : Bool
      @entries.any?(&.membership)
    end

    private def seeds_identified? : Bool
      @seed_ids.size == @seed_addresses.size
    end

    # Whether a member isn't connected from elsewhere than the membership (or
    # before there is one, the configured seeds) says it is. Only then do its
    # acks and votes count. Not being connected right now is fine: messages
    # only arrive over a connection, and a peer that restarted may not have
    # reconnected to us yet. Non-members, like a removed node acking its
    # removal, have no address to be at. After a long time without a leader
    # everyone counts, see @trust_moved.
    private def at_home?(id : Int32) : Bool
      return true if id == @id || @trust_moved
      address = @live[id]? || return true
      if m = latest_membership
        expected = m.addresses[id]? || return true
        expected == address
      else
        @seed_addresses.includes?(address)
      end
    end

    private def in_isr?(isr : Set(Int32)?, node_id : Int32) : Bool
      isr.nil? || isr.includes?(node_id)
    end

    # Without a log we can't tell whether we already voted in this term: our
    # raft state may have been lost while the clustering id was kept. Voting
    # again could elect a second leader, so only vote for candidates without a
    # log either, as when a cluster is first bootstrapped. A candidate with a
    # log is in the ISR, so it replicated from a leader, and the other voters
    # that did too have a log.
    private def may_vote_for_log?(candidate_last_index : Int64) : Bool
      last_index > 0 || candidate_last_index == 0
    end

    private def log_up_to_date?(index : Int64, term : Int64) : Bool
      term > last_term || (term == last_term && index >= last_index)
    end

    private def broadcast_append : Nil
      @peers.each { |p| send_append(p) }
      @departing.each_key { |p| send_append(p) }
    end

    private def expire_departing(now : Time::Instant) : Nil
      expired = @departing.select { |_, d| now >= d.deadline }.keys
      return if expired.empty?
      expired.each { |id| @departing.delete(id) }
      refresh_membership
    end

    private def tracked?(id : Int32) : Bool
      @peers.includes?(id) || @departing.has_key?(id)
    end

    private def voting_peers : Array(Int32)
      @peers.select { |p| @voters.includes?(p) }
    end

    # Whether a peer is a voter whose acks count
    private def counted?(peer : Int32) : Bool
      @voters.includes?(peer) && at_home?(peer)
    end

    # Our own vote, if we're a voter
    private def self_vote : Int32
      @voters.includes?(@id) ? 1 : 0
    end

    private def seed_membership : Membership
      addresses = {@id => @address}
      @seed_ids.each { |addr, id| addresses[id] = @live[id]? || addr }
      Membership.new(addresses.keys.to_set, Set(Int32).new, addresses)
    end

    # Recompute who we talk to and who counts after the log or snapshot changed.
    # Without a membership in the log the configured seeds are all voters.
    private def refresh_membership : Nil
      if m = latest_membership
        @voters = m.voters.dup
        @peers = m.members.reject(@id)
      else
        @voters = @seed_ids.values.to_set << @id
        @peers = @seed_ids.values.uniq!.reject(@id)
      end
      # Forget what we knew about peers that left, they may come back as new
      @next_index.reject! { |p, _| !tracked?(p) }
      @match_index.reject! { |p, _| !tracked?(p) }
      @last_ack.reject! { |p, _| !tracked?(p) }
      @answered.reject! { |p, _| !tracked?(p) }
      @peers.each do |p|
        next if @next_index.has_key?(p)
        @next_index[p] = last_index + 1
        @match_index[p] = 0i64
        @last_ack[p] = @now
      end
    end

    private def send_append(peer : Int32) : Nil
      next_index = @next_index[peer]? || last_index + 1
      if next_index <= @snapshot_index
        send peer, InstallSnapshot.new(@id, @term, @uri, @snapshot_index, @snapshot_term, @snapshot_isr,
          @snapshot_membership)
        return
      end
      prev = next_index - 1
      entries = @entries[(next_index - @snapshot_index - 1).to_i..]? || Array(Entry).new
      send peer, AppendEntries.new(@id, @term, @uri, prev, term_at(prev), entries, @snapshot_index)
    end

    private def advance_commit : Nil
      n = last_index
      while n > @snapshot_index
        break if term_at(n) != @term # only entries of the current term are committed by counting
        replicated = self_vote + @peers.count { |p| counted?(p) && (@match_index[p]? || 0i64) >= n }
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
      return if index <= @snapshot_index
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

    private def send(to : Int32, msg : Message) : Nil
      @outbox << {to, msg}
    end

    private def randomized_election_timeout : Time::Span
      @election_timeout + @election_timeout * @random.rand
    end
  end
end
