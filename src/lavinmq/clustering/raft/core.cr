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
    entries : Array(Entry), peer_node_ids = Hash(String, Int32).new

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
  #   is current, e.g. on the first start after migrating from etcd.
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
    @entries = Array(Entry).new
    @peers : Array(String)
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
      @peers = peers.reject(@id).uniq!
      if state
        @term = state.term
        @voted_for = state.voted_for
        @snapshot_index = state.snapshot_index
        @snapshot_term = state.snapshot_term
        @snapshot_isr = state.snapshot_isr
        @entries = state.entries.dup
        @peer_node_ids = state.peer_node_ids.dup
        @commit_index = @snapshot_index
      end
      @election_deadline = now + randomized_election_timeout
      @heartbeat_due = now
    end

    def hard_state : HardState
      HardState.new(@term, @voted_for, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup, @peer_node_ids.dup)
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

    # Leader whose no-op of this term is committed, i.e. it has applied every
    # entry committed by earlier leaders and may act on the ISR.
    def serving_leader? : Bool
      @role.leader? && @commit_index >= @term_start_index
    end

    def quorum : Int32
      (@peers.size + 1) // 2 + 1
    end

    # Append an ISR change. Returns its index, or nil when not the leader.
    # It's committed once `commit_index` reaches the index while still leader
    # in the same term.
    def propose(isr : Set(Int32), now : Time::Instant) : Int64?
      return unless @role.leader?
      append Entry.new(@term, isr)
      advance_commit
      broadcast_append
      last_index
    end

    def tick(now : Time::Instant) : Nil
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

    # Hand leadership to a fully caught up peer in the ISR. Returns false when
    # not leader or no peer is eligible.
    def transfer_leadership : Bool
      return false unless @role.leader?
      isr = latest_isr
      target = @peers.find do |p|
        @match_index[p]? == last_index &&
          (node_id = @peer_node_ids[p]?) && in_isr?(isr, node_id)
      end
      return false unless target
      send target, TimeoutNow.new(@id, @term)
      true
    end

    def step(msg : Message, now : Time::Instant) : Nil
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
        send msg.from, CatchUp.new(@id, @term, @node_id, @snapshot_index, @snapshot_term, @snapshot_isr, @entries.dup)
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

    private def handle_vote_response(msg : VoteResponse, now : Time::Instant) : Nil
      if msg.pre_vote
        return unless @pre_voting && msg.granted && msg.term == @term + 1
        @pre_votes << msg.from
        start_election(now, transfer: false) if @pre_votes.size >= quorum
        return
      end
      if msg.term > @term
        become_follower(msg.term, nil)
        return
      end
      return unless @role.candidate? && msg.term == @term && msg.granted
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
      install_snapshot(msg.index, msg.snapshot_term, msg.isr)
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
      install_snapshot(msg.snapshot_index, msg.snapshot_term, msg.snapshot_isr)
      merge_entries(msg.snapshot_index, msg.entries)
    end

    private def install_snapshot(index : Int64, term : Int64, isr : Set(Int32)?) : Nil
      return if index <= @commit_index
      @entries.clear
      @snapshot_index = index
      @snapshot_term = term
      @snapshot_isr = isr
      @commit_index = index
      @dirty = true
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
      @peers.each do |p|
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
      @peers.each do |p|
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
      @leader = leader
      @leader_uri = nil if leader.nil?
    end

    private def become_leader(now : Time::Instant) : Nil
      @role = Role::Leader
      @leader = @id
      @leader_uri = @uri
      @peers.each do |p|
        @next_index[p] = last_index + 1
        @match_index[p] = 0i64
        @last_ack[p] = now
      end
      append Entry.new(@term, latest_isr ? nil : Set{@node_id})
      @term_start_index = last_index
      advance_commit
      @heartbeat_due = now + @heartbeat_interval
      broadcast_append
    end

    private def lost_quorum?(now : Time::Instant) : Bool
      acked = 1
      @peers.each do |p|
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
    end

    private def send_append(peer : String) : Nil
      next_index = @next_index[peer]? || last_index + 1
      if next_index <= @snapshot_index
        send peer, InstallSnapshot.new(@id, @term, @node_id, @uri, @snapshot_index, @snapshot_term, @snapshot_isr)
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
        replicated = 1 + @peers.count { |p| (@match_index[p]? || 0i64) >= n }
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
      count = (index - @snapshot_index).to_i
      @entries.first(count).each { |e| e.isr.try { |s| isr = s } }
      @snapshot_term = term_at(index)
      @snapshot_isr = isr
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
    end

    private def truncate_from(index : Int64) : Nil
      keep = (index - @snapshot_index - 1).to_i
      return if keep >= @entries.size || keep < 0
      @entries.truncate(0, keep)
      @dirty = true
    end

    private def send(to : String, msg : Message) : Nil
      @outbox << {to, msg}
    end

    private def randomized_election_timeout : Time::Span
      @election_timeout + @election_timeout * @random.rand
    end
  end
end
