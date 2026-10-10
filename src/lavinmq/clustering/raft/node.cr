require "json"
require "./core"
require "./storage"
require "./transport"
require "../../bool_channel"
require "../../logger"

module LavinMQ::Clustering::Raft
  # A snapshot of the cluster as this node sees it, see Node#status. Only
  # the leader knows `match_index`, `caught_up` and `responsive`.
  record Status, id : Int32, address : String, role : Role, term : Int64,
    leader : Int32?, membership : Membership?,
    match_index : Hash(Int32, Int64), last_index : Int64, caught_up : Set(Int32),
    responsive : Set(Int32), committed_isr : Set(Int32)?, leader_heard_ago : Time::Span? = nil do
    # The member a clustering id (base 36, as shown) or a raft address refers to
    def resolve(ref : String) : Int32?
      members = membership.try(&.addresses) || {@id => @address}
      if id = ref.to_i?(36)
        return id if members.has_key?(id)
      end
      members.key_for?(ref)
    end

    def address_of(id : Int32) : String?
      membership.try(&.addresses[id]?) || (@address if id == @id)
    end

    # As served by /api/cluster. A node that isn't the leader only shows its
    # own view (`local`): the leader it knows of, if any, and no progress.
    def to_json(json : JSON::Builder) : Nil
      leading = role.leader?
      json.object do
        json.field "leader", leader.try { |l| address_of(l) || l.to_s(36) }
        json.field "term", term
        unless leading
          json.field "local", true
          json.field "node", address
          json.field "role", role.to_s.downcase
          json.field "leader_heard_ago_ms", leader_heard_ago.try(&.total_milliseconds.to_i64)
        end
        json.field "isr" do
          json.array { committed_isr.try &.each { |id| json.string id.to_s(36) } }
        end
        json.field "members" do
          json.array do
            addresses = membership.try(&.addresses) || {id => address}
            addresses.to_a.sort_by!(&.[1]).each do |member, addr|
              me = member == id
              json.object do
                json.field "address", addr
                json.field "node_id", member.to_s(36)
                json.field "role", membership.try(&.learners.includes?(member)) ? "learner" : "voter"
                json.field "in_isr", committed_isr.try(&.includes?(member)) || false
                json.field "match_index", leading ? (me ? last_index : match_index[member]?) : nil
                json.field "caught_up", leading ? (me || caught_up.includes?(member)) : nil
                json.field "leader", member == leader
              end
            end
          end
        end
      end
    end
  end

  # Counters and gauges for Prometheus, see Node#metrics. `peers` maps every
  # other member to whether it's connected, `leader_contact` is zero on the
  # leader and nil on a node that hasn't heard from one. `save_buckets` are
  # per bucket of Node::SAVE_BUCKETS (not cumulative), plus one for slower saves.
  record Metrics, leader : Int32?, is_leader : Bool, term : Int64, leader_changes : UInt64,
    leader_contact : Time::Span?, proposals_pending : Int64, peers : Hash(Int32, Bool),
    isr_size : Int32?, save_buckets : Array(UInt64), save_count : UInt64, save_sum : Float64

  # Runs a Core in a single fiber: every message, request and tick goes
  # through @events, so the Core needs no locking. State is persisted before
  # any message produced alongside it leaves the node. The fiber runs in
  # *execution_context*; RaftController gives it a context of its own, so that
  # a busy broker can't delay heartbeats and votes.
  class Node
    Log = LavinMQ::Log.for "clustering.raft"

    private record Propose, isr : Set(Int32), reply : Channel(Bool)
    private record ChangeMembership, change : MembershipChange, id : Int32, address : String?,
      reply : Channel(MembershipError?)
    private record Transfer, target : Int32?, reply : Channel(TransferResult)
    private record CheckTransfer, target : Int32?, reply : Channel(Tuple(Int32, Int64) | TransferRefusal)
    private record GetStatus, reply : Channel(Status)
    private record GetMetrics, reply : Channel(Metrics)
    # A proposal that's answered once its entry is committed, or lost
    private record Pending, index : Int64, term : Int64, done : Bool ->
    private alias Event = TransportEvent | Propose | ChangeMembership | Transfer | CheckTransfer | GetStatus | GetMetrics

    # True while this node is the leader and has committed an entry in its
    # term, i.e. it knows the latest committed ISR.
    getter serving = BoolChannel.new(false)
    # Notified (non-blocking, coalesced) whenever `leader_uri` changes.
    getter leader_changed = Channel(Nil).new(1)

    EVENT_QUEUE_SIZE = 256
    # Upper bounds, in seconds, of the storage save (fsync) histogram
    SAVE_BUCKETS = {0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048, 4.096, 8.192}

    @events = Channel(Event).new(EVENT_QUEUE_SIZE)
    @pending = Array(Pending).new
    @leader_uri : String? = nil
    @term = 0i64
    @leader = false
    @committed_isr : Set(Int32)? = nil
    @membership : Membership? = nil
    @committed_membership : Membership? = nil
    @removed_callbacks = Array(Int32 ->).new
    @state_lock = Mutex.new
    @stopped = Channel(Nil).new
    @transport : Transport? = nil
    @synced_peers = Set(String).new
    @seeds : Set(String)
    @logged_seeds = false
    @election_timeout : Time::Span
    @logged_trust_moved = false
    # Metrics, only touched by the event loop
    # The leader and term last counted as a leader change: the same node
    # elected again in a later term is a change too
    @last_leader : Tuple(Int32, Int64)? = nil
    @leader_changes = 0_u64
    @save_buckets = Array(UInt64).new(SAVE_BUCKETS.size + 1, 0_u64)
    @save_count = 0_u64
    @save_sum = 0.0

    def initialize(@id : Int32, @address : String, seeds : Enumerable(String), uri : String,
                   @storage : Storage, election_timeout : Time::Span, heartbeat_interval : Time::Span,
                   bootstrap = false,
                   @execution_context : Fiber::ExecutionContext = Fiber::ExecutionContext.current)
      @election_timeout = election_timeout
      @core = Core.new(@id, @address, seeds, uri, election_timeout, heartbeat_interval,
        Time.instant, @storage.load, bootstrap: bootstrap)
      @seeds = seeds.to_set << @address
      @term = @core.term
      @committed_isr = @core.committed_isr
      @membership = @core.latest_membership
      @committed_membership = @core.committed_membership
    end

    def run(transport : Transport) : Nil
      @transport = transport
      @execution_context.spawn(name: "raft node") { event_loop }
    end

    # Called by the transport for every received message and connection change.
    def deliver(event : TransportEvent) : Nil
      @events.send event
    rescue Channel::ClosedError
    end

    def leader_uri : String?
      @state_lock.synchronize { @leader_uri }
    end

    def leader? : Bool
      @state_lock.synchronize { @leader }
    end

    def term : Int64
      @state_lock.synchronize { @term }
    end

    def committed_isr : Set(Int32)?
      @state_lock.synchronize { @committed_isr }
    end

    # The membership in effect, committed or not. Nil until a leader has
    # seeded it.
    def membership : Membership?
      @state_lock.synchronize { @membership }
    end

    # Whether the node with this clustering id is part of the cluster, as
    # voter or learner. True while there's no membership yet.
    def member?(id : Int32) : Bool
      return true if id == @id
      @state_lock.synchronize { (m = @membership).nil? || m.includes?(id) }
    end

    # False once this node has been removed from the cluster.
    def self_member? : Bool
      @state_lock.synchronize { (m = @membership).nil? || m.includes?(@id) }
    end

    # Called, in a fiber of its own, with the clustering id of every node
    # that a committed membership change removed.
    def on_member_removed(&block : Int32 ->) : Nil
      @state_lock.synchronize { @removed_callbacks << block }
    end

    # Replicate an ISR change. Blocks until committed (true) or until this
    # node stops being the leader of the term it was proposed in (false).
    def propose_isr(isr : Set(Int32)) : Bool
      reply = Channel(Bool).new(1)
      @events.send Propose.new(isr, reply)
      await reply, false
    rescue Channel::ClosedError
      false
    end

    # Add the node at `address` as non-voting learner. It must be running, so
    # its clustering id can be asked for. Each of the membership changes blocks
    # until the change is committed and returns nil, or returns why it was
    # refused (or lost, see MembershipError::Lost).
    def add_learner(address : String) : MembershipError?
      return MembershipError::NotLeader unless leader?
      id = @transport.try(&.probe(address)) || return MembershipError::Unreachable
      change MembershipChange::AddLearner, id, address
    end

    # Make a learner that has caught up a voter.
    def promote(id : Int32) : MembershipError?
      change MembershipChange::Promote, id
    end

    def remove_member(id : Int32) : MembershipError?
      change MembershipChange::Remove, id
    end

    private def change(change : MembershipChange, id : Int32, address : String? = nil) : MembershipError?
      reply = Channel(MembershipError?).new(1)
      @events.send ChangeMembership.new(change, id, address, reply)
      await reply, MembershipError::Lost
    rescue Channel::ClosedError
      MembershipError::NotLeader
    end

    # Hand leadership over to a caught up in-sync voter, the chosen one or any,
    # so the cluster fails over without waiting for an election timeout.
    def transfer_leadership(target : Int32? = nil) : TransferResult
      reply = Channel(TransferResult).new(1)
      @events.send Transfer.new(target, reply)
      await reply, TransferResult::NotLeader
    rescue Channel::ClosedError
      TransferResult::NotLeader
    end

    # The voter #transfer_leadership would hand over to, with the term it was
    # checked in, or why it can't, see Core#transfer_check.
    def transfer_check(target : Int32? = nil) : Tuple(Int32, Int64) | TransferRefusal
      reply = Channel(Tuple(Int32, Int64) | TransferRefusal).new(1)
      @events.send CheckTransfer.new(target, reply)
      await reply, TransferRefusal::NotLeader
    rescue Channel::ClosedError
      TransferRefusal::NotLeader
    end

    # The cluster as this node sees it right now. Built on demand, in the
    # event loop, so it's consistent. Nil when the node has stopped.
    def status : Status?
      reply = Channel(Status).new(1)
      @events.send GetStatus.new(reply)
      select
      when s = reply.receive
        s
      when @stopped.receive?
        nil
      end
    rescue Channel::ClosedError
      nil
    end

    # Built in the event loop, like #status. Nil when the node has stopped or
    # doesn't answer within a second, e.g. stuck in an fsync. The second
    # includes queueing the request, the queue fills up while it's stuck.
    def metrics : Metrics?
      deadline = Time.instant + 1.second
      reply = Channel(Metrics).new(1)
      select
      when @events.send(GetMetrics.new(reply))
      when timeout(deadline - Time.instant)
        return
      end
      select
      when m = reply.receive
        m
      when @stopped.receive?
        nil
      when timeout(deadline - Time.instant)
        nil
      end
    rescue Channel::ClosedError
      nil
    end

    private def await(reply : Channel(T), default : T) : T forall T
      select
      when result = reply.receive
        result
      when @stopped.receive?
        default
      end
    end

    def close : Nil
      @events.close
      if @transport
        @stopped.receive?
      else
        @stopped.close
      end
      @transport.try &.close
    end

    # Sleeps until a message or request arrives, or until the Core's next
    # deadline, e.g. a heartbeat or an election timeout.
    private def event_loop : Nil
      loop do
        wait = @core.next_deadline(Time.instant) - Time.instant
        if wait > Time::Span.zero
          select
          when event = @events.receive?
            break unless event
            handle(event)
          when timeout(wait)
          end
        end
        break unless handle_queued
        @core.tick(Time.instant)
        flush
      end
    ensure
      @pending.each &.done.call(false)
      @pending.clear
      @serving.set(false)
      @stopped.close
    end

    # Handles what arrived while this fiber was busy, e.g. in an fsync, before
    # the next tick: a leader stalled past the election timeout would
    # otherwise step down with its followers' acks still queued. Bounded so
    # ticks keep going under a steady stream. Returns false once closed.
    private def handle_queued : Bool
      EVENT_QUEUE_SIZE.times do
        select
        when event = @events.receive?
          return false unless event
          handle(event)
        else
          return true
        end
      end
      true
    end

    private def handle(event : Event) : Nil
      case event
      in Message
        @core.step(event, Time.instant)
      in Connected
        Log.debug { "#{event.address} (#{event.id.to_s(36)}) connected" }
        @core.connected(event.id, event.address, Time.instant)
      in Disconnected
        @core.disconnected(event.id, event.address)
      in Identified
        @core.identified(event.address, event.id)
      in Propose
        if index = @core.propose(event.isr, Time.instant)
          @pending << pending(index, event.reply)
        else
          event.reply.send false
        end
      in ChangeMembership
        case result = @core.propose_membership(event.change, event.id, Time.instant, event.address)
        in Int64
          @pending << pending(result, event.reply)
        in MembershipError
          event.reply.send result
        end
      in Transfer
        event.reply.send @core.transfer_leadership(event.target)
      in CheckTransfer
        result = @core.transfer_check(event.target)
        event.reply.send result.is_a?(Int32) ? {result, @core.term} : result
      in GetStatus
        event.reply.send build_status
      in GetMetrics
        event.reply.send build_metrics
      end
    end

    private def build_status : Status
      match_index = Hash(Int32, Int64).new
      caught_up = Set(Int32).new
      responsive = Set(Int32).new
      if @core.role.leader?
        @core.peers.each do |p|
          match_index[p] = @core.match_index(p)
          caught_up << p if @core.caught_up?(p)
          responsive << p if @core.responsive?(p)
        end
      end
      Status.new(@id, @address, @core.role, @core.term, @core.leader,
        @core.latest_membership, match_index,
        @core.last_index, caught_up, responsive, @core.committed_isr, @core.leader_heard_ago(Time.instant))
    end

    private def build_metrics : Metrics
      peers = Hash(Int32, Bool).new
      @core.peers.each { |p| peers[p] = @core.connected?(p) }
      contact = @core.role.leader? ? Time::Span.zero : @core.leader_heard_ago(Time.instant)
      Metrics.new(@core.leader, @core.role.leader?, @core.term, @leader_changes, contact,
        proposals_pending, peers, @core.committed_isr.try(&.size),
        @save_buckets.dup, @save_count, @save_sum)
    end

    # Entries the leader has appended but not committed, whether proposed by
    # a caller or by the Core itself (membership seeding, relocation)
    private def proposals_pending : Int64
      @core.role.leader? ? @core.last_index - @core.commit_index : 0_i64
    end

    private def observe_save(seconds : Float64) : Nil
      bucket = SAVE_BUCKETS.index { |le| seconds <= le } || SAVE_BUCKETS.size
      @save_buckets[bucket] += 1
      @save_count += 1
      @save_sum += seconds
    end

    private def flush : Nil
      if @core.dirty?
        begin
          started = Time.instant
          @storage.save(@core.hard_state)
          observe_save((Time.instant - started).total_seconds)
        rescue ex
          Log.fatal(exception: ex) { "Could not persist raft state to #{@storage.path}" }
          exit 1
        end
        @core.persisted
      end
      if transport = @transport
        @core.take_outbox.each do |(to, msg)|
          @core.address_of(to).try { |address| transport.send(address, msg) }
        end
      end
      resolve_pending
      publish_state
      sync_transport
    end

    private def pending(index : Int64, reply : Channel(Bool)) : Pending
      Pending.new(index, @core.term, ->(committed : Bool) { reply.send committed })
    end

    private def pending(index : Int64, reply : Channel(MembershipError?)) : Pending
      Pending.new(index, @core.term, ->(committed : Bool) { reply.send(committed ? nil : MembershipError::Lost) })
    end

    private def resolve_pending : Nil
      return if @pending.empty?
      @pending.reject! do |p|
        if p.term == @core.term && @core.commit_index >= p.index
          p.done.call(true)
          true
        elsif p.term != @core.term || !@core.role.leader?
          p.done.call(false)
          true
        else
          false
        end
      end
    end

    private def sync_transport : Nil
      transport = @transport || return
      wanted = @core.connect_to
      return if wanted == @synced_peers
      @synced_peers = wanted
      transport.update_peers(wanted)
    end

    private def publish_state : Nil
      if (leader_id = @core.leader) && {leader_id, @core.term} != @last_leader
        @last_leader = {leader_id, @core.term}
        @leader_changes += 1
      end
      uri = @core.leader_uri
      term = @core.term
      isr = @core.committed_isr
      leader = @core.role.leader?
      membership = @core.latest_membership
      committed = @core.committed_membership
      changed = false
      removed = Array(Int32).new
      callbacks = nil
      @state_lock.synchronize do
        changed = uri != @leader_uri || leader != @leader
        @leader_uri = uri
        @term = term
        @leader = leader
        @committed_isr = isr
        @membership = membership
        if committed != @committed_membership
          before = @committed_membership
          log_membership_change(before, committed)
          if before && committed
            removed.concat(before.members - committed.members)
          end
          @committed_membership = committed
          callbacks = @removed_callbacks.dup unless removed.empty?
        end
      end
      if changed
        Log.info { uri ? "Leader: #{uri} (term #{@core.term})" : "No leader (term #{@core.term})" }
        select
        when @leader_changed.send(nil)
        else
        end
      end
      log_seeds_if_different(committed)
      log_trust_moved
      callbacks.try do |cbs|
        removed.each do |id|
          Log.info { "Node #{id.to_s(36)} was removed from the cluster" }
          # Broker code, keep it off the raft context
          cbs.each { |cb| Fiber::ExecutionContext.default.spawn(name: "raft member removed") { cb.call(id) } }
        end
      end
      @serving.swap(@core.serving_leader?)
    end

    # Voters at other addresses than the membership lists are only counted
    # after a long time without a leader, see Core#trusting_moved?.
    private def log_trust_moved : Nil
      trusting = @core.trusting_moved?
      return if trusting == @logged_trust_moved
      @logged_trust_moved = trusting
      if trusting
        moved = @core.moved_from.try { |from| ", including this node, which the membership lists at #{from}" } || ""
        Log.warn do
          "No leader for #{@election_timeout * Core::MOVED_TRUST_AFTER}: counting voters at other addresses " \
          "than the cluster membership lists#{moved}. The leader elected records the new addresses, " \
          "see \"Changing a node's address\" in docs/clustering.md"
        end
      else
        Log.info { "Leader found, counting only voters at their listed addresses again" }
      end
    end

    private def log_membership_change(before : Membership?, after : Membership?) : Nil
      return unless before && after
      after.addresses.each do |id, address|
        old = before.addresses[id]?
        next if old.nil? || old == address
        Log.info { "Node #{id.to_s(36)} moved from #{old} to #{address}" }
      end
    end

    # Seeds that differ from the membership are normal, e.g. on a node that
    # joined through one member, but worth knowing about.
    private def log_seeds_if_different(membership : Membership?) : Nil
      return if @logged_seeds || membership.nil?
      @logged_seeds = true
      return if membership.addresses.values.to_set == @seeds
      Log.info do
        "Using the cluster membership from the raft log (voters: #{describe(membership, membership.voters)}; " \
        "learners: #{describe(membership, membership.learners)}), the configured seeds " \
        "(#{@seeds.to_a.sort.join(", ")}) are only used to form or join a cluster"
      end
    end

    private def describe(membership : Membership, ids : Set(Int32)) : String
      ids.map { |id| "#{membership.addresses[id]? || "?"} (#{id.to_s(36)})" }.sort!.join(", ")
    end
  end
end
