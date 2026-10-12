require "./messages"
require "./log_store"

module LavinMQ::Clustering::Raft
  enum Role
    Follower
    Candidate
    Leader
  end

  # What must be on disk before any message produced alongside it is sent.
  # `entries` are only here when the log is kept in memory, a journal keeps
  # them otherwise, and `journal_generation` says which, see JournalLogStore.
  record HardState, term : Int64, voted_for : Int32?,
    snapshot_index : Int64, snapshot_term : Int64, snapshot_isr : Set(Int32)?,
    entries : Array(Entry), snapshot_membership : Membership? = nil, journal_generation : Int64 = 0i64

  enum TransferResult
    # TimeoutNow was sent to the target
    Sent
    # The target is behind, TimeoutNow is sent when it has caught up
    Pending
    NotLeader
    # See TransferRefusal for why
    NotEligible
  end

  # Why a leadership transfer can't be done, see Core#transfer_check
  enum TransferRefusal
    NotLeader
    # The leader hasn't committed an entry in its term, so it doesn't know
    # the latest ISR
    NotServing
    IsLeader
    NotMember
    NotVoter
    NotInIsr
    # Hasn't answered the leader within the election timeout, or isn't
    # connected from where the membership lists it
    Unresponsive
    # Without a target: no voter is eligible
    NoEligibleVoter

    # *target* as the transfer was requested to it
    def message(target : String?) : String
      case self
      in NotLeader       then "This node is not the leader"
      in NotServing      then "The leader hasn't committed an entry in its term yet"
      in IsLeader        then "#{target} is the leader"
      in NotMember       then "#{target} is not a member"
      in NotVoter        then "#{target} is not a voter"
      in NotInIsr        then "#{target} is not in the in-sync replica set"
      in Unresponsive    then "#{target} hasn't answered the leader recently"
      in NoEligibleVoter then "No reachable voter is in the in-sync replica set"
      end
    end
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
    # The compacted log, see #compact_to: the log up to and including `index`,
    # folded into the ISR and membership it ended with
    private record Snapshot, index : Int64, term : Int64, isr : Set(Int32)?, membership : Membership?

    # What the leader knows about a peer, and what it has sent it.
    #
    # A peer is probed until it accepts an append: one message at a time,
    # walking next_index back on rejections. Then entries are replicated to
    # it optimistically, next_index moving on as they're sent, with a bounded
    # number of messages and bytes in flight. A rejection, or no progress for
    # #resend_after, makes it probed again from what it has.
    private class Progress
      # The next entry to send it
      property next_index : Int64
      # The last entry it's known to have
      property match_index = 0i64
      # When it last acked this leader, or since when this leader has
      # counted it: check-quorum gives it an election timeout from then
      property last_ack : Time::Instant
      # When it last answered this leader, unlike last_ack never seeded
      property answered : Time::Instant? = nil
      property? probing = true
      # A probe is out, the next goes when it's answered or on a heartbeat
      property? probe_sent = false
      # The last index of each message in flight, with its size
      getter inflight = Deque(Tuple(Int64, Int64)).new
      property inflight_bytes = 0i64
      # When match_index last moved, or replication (re)started
      property progressed : Time::Instant

      def initialize(@next_index : Int64, @last_ack : Time::Instant)
        @progressed = @last_ack
      end

      # Forgets the messages in flight up to *index*, it has those
      def acked(index : Int64) : Nil
        while (first = @inflight.first?) && first[0] <= index
          @inflight.shift
          @inflight_bytes -= first[1]
        end
      end

      # Back to probing, from *next_index*
      def probe(next_index : Int64) : Nil
        @probing = true
        @probe_sent = false
        @next_index = next_index
        @inflight.clear
        @inflight_bytes = 0i64
      end
    end

    # Messages and bytes in flight to a peer being replicated to
    MAX_INFLIGHT       = 8
    MAX_INFLIGHT_BYTES = 16i64 << 20
    # Entries in one AppendEntries, by their encoded size, at least one entry
    # is always sent. Below Codec::MAX_FRAME, which the receiver enforces.
    MAX_APPEND_BYTES = 512i64 << 10

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
    # The state changed in a way that must be saved before any message
    # produced alongside it leaves the node
    getter? dirty = false
    # The term, the vote or the snapshot changed, unlike only the log
    getter? meta_dirty = false
    # Only committed entries were folded into the snapshot since the last
    # save. They were saved when appended and a majority has them, so a node
    # that crashes before saving this just finds them uncommitted, and they
    # can't be lost: nothing has to wait for it.
    getter? unsaved_commit = false
    # Compact the log as soon as entries are committed. The ISR and the
    # membership are all the state there is, so nothing needs the entries
    # after that, unless they carry data a follower may still need.
    property? compact_on_commit = true

    @snapshot = Snapshot.new(0i64, 0i64, nil, nil)
    # The entries after the snapshot, committed or not
    @log : LogStore
    getter commit_index = 0i64
    # The ISR and the membership as of the commit index
    getter committed_isr : Set(Int32)? = nil
    getter committed_membership : Membership? = nil
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
    @progress = Hash(Int32, Progress).new
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
                   @bootstrap = false, log : LogStore? = nil)
      @seed_addresses = seeds.reject(@address).uniq!
      @now = now
      @leaderless_since = now
      if state
        @term = state.term
        @voted_for = state.voted_for
        @snapshot = Snapshot.new(state.snapshot_index, state.snapshot_term, state.snapshot_isr,
          state.snapshot_membership)
      end
      @log = log || MemoryLogStore.new(@snapshot.index, @snapshot.term, state.try(&.entries) || Array(Entry).new)
      unless @log.snapshot_index == @snapshot.index
        raise ArgumentError.new("The log starts after #{@log.snapshot_index}, the snapshot is at #{@snapshot.index}")
      end
      # What's committed beyond the snapshot is learnt from the leader again
      @commit_index = @snapshot.index
      @committed_isr = @snapshot.isr
      @committed_membership = @snapshot.membership
      @election_deadline = now + randomized_election_timeout
      @heartbeat_due = now
      refresh_membership
    end

    def hard_state : HardState
      HardState.new(@term, @voted_for, @snapshot.index, @snapshot.term, @snapshot.isr, @log.entries,
        @snapshot.membership)
    end

    # The hard state without the log, for when the log persists itself
    def meta_state : HardState
      HardState.new(@term, @voted_for, @snapshot.index, @snapshot.term, @snapshot.isr, Array(Entry).new,
        @snapshot.membership)
    end

    def persisted : Nil
      @dirty = @meta_dirty = @unsaved_commit = false
    end

    def take_outbox : Array(Tuple(Int32, Message))
      msgs = @outbox
      @outbox = Array(Tuple(Int32, Message)).new
      msgs
    end

    def last_index : Int64
      @log.last_index
    end

    def last_term : Int64
      @log.term_at(last_index)
    end

    # The ISR of the latest entry in the log, committed or not.
    def latest_isr : Set(Int32)?
      @log.reverse_each { |e| e.isr.try { |isr| return isr } }
      @snapshot.isr
    end

    # The membership of the latest entry in the log, committed or not. It's
    # nil until a leader has made it from its seeds.
    def latest_membership : Membership?
      @log.reverse_each { |e| e.membership.try { |m| return m } }
      @snapshot.membership
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
      @progress[id]?.try(&.match_index) || 0i64
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
      answered = @progress[id]?.try(&.answered) || return false
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
      @role.leader? && @commit_index >= @term_start_index
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

    # Append an entry with the broker's *data*. Returns its index, or nil when
    # not the leader. Committed like #propose.
    def propose_data(data : Bytes, now : Time::Instant) : Int64?
      return unless @role.leader?
      @now = now
      append Entry.new(@term, nil, nil, data)
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
      # Dropped with the entry that removes it, but it's still told so
      progress = @progress[id]?
      append Entry.new(@term, isr, Membership.new(voters, learners, addresses, relocated))
      if departing_address
        @departing[id] = Departing.new(last_index, now + @election_timeout * 5, departing_address)
        @progress[id] = progress || Progress.new(last_index, now)
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
          heartbeat(now)
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
          expires = (@progress[p]? || next).last_ack + @election_timeout
          deadline = expires if now < expires < deadline
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
      case peer = transfer_check(target)
      in TransferRefusal
        peer.not_leader? ? TransferResult::NotLeader : TransferResult::NotEligible
      in Int32
        if match_index(peer) == last_index
          @transfer_target = nil
          send peer, TimeoutNow.new(@id, @term)
          TransferResult::Sent
        else
          @transfer_target = {peer, @now + @election_timeout}
          send_append(peer)
          TransferResult::Pending
        end
      end
    end

    # Who #transfer_leadership would hand leadership to, or why it can't,
    # without doing it. The target must be a voter in the ISR that answers
    # this leader. Without one, a fully caught up such voter is preferred.
    def transfer_check(target : Int32? = nil) : Int32 | TransferRefusal
      return TransferRefusal::NotLeader unless @role.leader?
      return TransferRefusal::NotServing unless serving_leader?
      if target
        return TransferRefusal::IsLeader if target == @id
        transfer_refusal(target) || target
      else
        @peers.find { |p| transfer_refusal(p).nil? && match_index(p) == last_index } ||
          @peers.find { |p| transfer_refusal(p).nil? } ||
          TransferRefusal::NoEligibleVoter
      end
    end

    private def transfer_refusal(peer : Int32) : TransferRefusal?
      return TransferRefusal::NotMember unless @peers.includes?(peer)
      return TransferRefusal::NotVoter unless @voters.includes?(peer)
      return TransferRefusal::NotInIsr unless latest_isr.try(&.includes?(peer))
      TransferRefusal::Unresponsive unless at_home?(peer) && responsive?(peer)
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
        # TODO: bound this to a frame before entries carry data
        send msg.from, CatchUp.new(@id, @term, @snapshot.index, @snapshot.term, @snapshot.isr, @log.entries,
          @snapshot.membership)
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
        @dirty = @meta_dirty = true
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
      if msg.prev_index >= @snapshot.index && term_at(msg.prev_index) != msg.prev_term
        # Conflicting entries are never committed, drop them
        truncate_from(msg.prev_index)
        send msg.from, AppendResponse.new(@id, @term, false, msg.prev_index - 1)
        return
      end
      merge_entries(msg.prev_index, msg.entries)
      match = msg.prev_index + msg.entries.size
      if msg.commit > @commit_index
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

    # Everything up to *index* is committed and folded into *isr* and
    # *membership*. Our log is dropped, the leader sends what follows.
    private def install_snapshot(index : Int64, term : Int64, isr : Set(Int32)?, membership : Membership?) : Nil
      return if index <= @commit_index
      @log.reset(index, term)
      @snapshot = Snapshot.new(index, term, isr, membership)
      @commit_index = index
      @committed_isr = isr
      @committed_membership = membership
      @dirty = @meta_dirty = true
      refresh_membership
    end

    private def merge_entries(prev_index : Int64, entries : Array(Entry)) : Nil
      entries.each_with_index do |entry, i|
        index = prev_index + 1 + i
        # Committed, so we have it already
        next if index <= @commit_index
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
      # Not a peer, nor departing: removed, and has been told so
      progress = @progress[msg.from]? || return
      progress.last_ack = progress.answered = now
      if msg.success
        if msg.match_index > progress.match_index
          progress.match_index = msg.match_index
          progress.progressed = now
          advance_commit
        end
        progress.acked(msg.match_index)
        if progress.probing?
          progress.probing = false
          progress.next_index = Math.max(progress.next_index, msg.match_index + 1)
        end
        send_append(msg.from)
        if (d = @departing[msg.from]?) && msg.match_index >= d.index
          @departing.delete(msg.from)
          refresh_membership
        end
        if (t = @transfer_target) && t[0] == msg.from && msg.match_index == last_index
          @transfer_target = nil
          send msg.from, TimeoutNow.new(@id, @term)
        end
      else
        # The hint is the last entry it has, or one before the one it lacks
        next_index = Math.min(progress.next_index - 1, msg.match_index + 1)
        progress.probe(Math.max(progress.match_index + 1, next_index))
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
      @dirty = @meta_dirty = true
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
        @dirty = @meta_dirty = true
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
      @progress.clear
      @peers.each { |p| @progress[p] = Progress.new(last_index + 1, now) }
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
        counted?(p) && (pr = @progress[p]?) && now - pr.last_ack < @election_timeout
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
      (@commit_index + 1..last_index).any? { |i| @log.entry(i).membership }
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

    # Sends what each peer can take of the new entries
    private def broadcast_append : Nil
      @peers.each { |p| send_append(p) }
      @departing.each_key { |p| send_append(p) }
    end

    # Every peer hears from the leader, also when there's nothing new to send
    # it. A probe that wasn't answered goes again, and so do entries in flight
    # without progress for #resend_after, two heartbeats.
    private def heartbeat(now : Time::Instant) : Nil
      @progress.each do |peer, progress|
        next unless tracked?(peer)
        if progress.probing?
          progress.probe_sent = false
        elsif !progress.inflight.empty? && now - progress.progressed >= resend_after
          progress.probe(progress.match_index + 1)
        end
        send_append(peer, heartbeat: true)
      end
    end

    # How long entries in flight may go unacknowledged before they're sent
    # again, as lost
    private def resend_after : Time::Span
      @heartbeat_interval * 2
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
      @progress.reject! { |p, _| !tracked?(p) }
      @peers.each { |p| @progress[p] ||= Progress.new(last_index + 1, @now) }
    end

    # Sends a peer the entries it can take, see Progress. With *heartbeat*
    # something is sent also when no entries are: an empty append after what
    # it's known to have, which can't make it truncate anything.
    private def send_append(peer : Int32, heartbeat = false) : Nil
      progress = @progress[peer]? || return
      if progress.next_index <= @snapshot.index
        send peer, InstallSnapshot.new(@id, @term, @uri, @snapshot.index, @snapshot.term, @snapshot.isr,
          @snapshot.membership)
        return
      end
      sent = false
      if progress.probing?
        unless progress.probe_sent?
          send_entries(peer, progress.next_index)
          progress.probe_sent = true
          sent = true
        end
      else
        while progress.next_index <= last_index && progress.inflight.size < MAX_INFLIGHT &&
              progress.inflight_bytes < MAX_INFLIGHT_BYTES
          last, bytes = send_entries(peer, progress.next_index)
          progress.inflight << {last, bytes}
          progress.inflight_bytes += bytes
          progress.next_index = last + 1
          sent = true
        end
      end
      return if sent || !heartbeat
      prev = Math.max(progress.match_index, @snapshot.index)
      send peer, AppendEntries.new(@id, @term, @uri, prev, term_at(prev), Array(Entry).new, @commit_index)
    end

    # One AppendEntries from *from* on, as many entries as fit in
    # MAX_APPEND_BYTES but at least one. Returns the last index sent and the
    # size of the entries.
    private def send_entries(peer : Int32, from : Int64) : Tuple(Int64, Int64)
      entries = Array(Entry).new
      bytes = 0i64
      index = from
      while index <= last_index
        entry = @log.entry(index)
        size = entry_size(entry)
        break if !entries.empty? && bytes + size > MAX_APPEND_BYTES
        entries << entry
        bytes += size
        index += 1
      end
      prev = from - 1
      send peer, AppendEntries.new(@id, @term, @uri, prev, term_at(prev), entries, @commit_index)
      {prev + entries.size, bytes}
    end

    # Roughly what an entry takes in an AppendEntries
    private def entry_size(entry : Entry) : Int64
      size = 32i64
      entry.data.try { |d| size += d.size }
      entry.isr.try { |i| size += 4 * i.size }
      entry.membership.try { |m| size += 64 * m.addresses.size }
      size
    end

    private def advance_commit : Nil
      n = last_index
      while n > @commit_index
        break if term_at(n) != @term # only entries of the current term are committed by counting
        replicated = self_vote + @peers.count { |p| counted?(p) && match_index(p) >= n }
        if replicated >= quorum
          commit_to n
          return
        end
        n -= 1
      end
    end

    private def commit_to(index : Int64) : Nil
      index = Math.min(index, last_index)
      return if index <= @commit_index
      (@commit_index + 1..index).each do |i|
        e = @log.entry(i)
        e.isr.try { |isr| @committed_isr = isr }
        e.membership.try { |m| @committed_membership = m }
      end
      @commit_index = index
      compact_to(index) if @compact_on_commit
    end

    # Folds the committed entries up to *index* into the snapshot and drops
    # them from the log
    def compact_to(index : Int64) : Nil
      index = Math.min(index, @commit_index)
      return if index <= @snapshot.index
      isr = @snapshot.isr
      membership = @snapshot.membership
      (@snapshot.index + 1..index).each do |i|
        e = @log.entry(i)
        e.isr.try { |s| isr = s }
        e.membership.try { |m| membership = m }
      end
      term = term_at(index)
      @snapshot = Snapshot.new(index, term, isr, membership)
      @log.compact_to(index, term)
      @unsaved_commit = true
    end

    private def term_at(index : Int64) : Int64
      @log.term_at(index)
    end

    private def append(entry : Entry) : Nil
      @log.append entry
      @dirty = true
      refresh_membership if entry.membership
    end

    # Committed entries are never truncated: a leader that conflicts with
    # them can't have been elected
    private def truncate_from(index : Int64) : Nil
      return if index <= @commit_index || index > last_index
      reverted = (index..last_index).any? { |i| @log.entry(i).membership }
      @log.truncate_from(index)
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
