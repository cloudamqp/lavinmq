require "./core"
require "./storage"
require "./transport"
require "../../bool_channel"
require "../../logger"

module LavinMQ::Clustering::Raft
  # A snapshot of the cluster as this node sees it, see Node#status. Only
  # the leader knows `match_index` and `caught_up`.
  record Status, address : String, node_id : Int32, role : Role, term : Int64,
    leader : String?, leader_uri : String?,
    membership : Membership?, committed_membership : Membership?,
    peer_node_ids : Hash(String, Int32), match_index : Hash(String, Int64),
    last_index : Int64, caught_up : Set(String), committed_isr : Set(Int32)? do
    # The clustering id of a member, ours included
    def node_id_of(addr : String) : Int32?
      addr == @address ? @node_id : @peer_node_ids[addr]?
    end
  end

  # Runs a Core in a single fiber: every message, request and tick goes
  # through @events, so the Core needs no locking. State is persisted before
  # any message produced alongside it leaves the node.
  class Node
    Log = LavinMQ::Log.for "clustering.raft"

    private record Propose, isr : Set(Int32), reply : Channel(Bool)
    private record ChangeMembership, change : MembershipChange, addr : String, reply : Channel(MembershipError?)
    private record Transfer, target : String?, reply : Channel(TransferResult)
    private record GetStatus, reply : Channel(Status)
    private record Pending, index : Int64, term : Int64, reply : Channel(Bool)
    private record PendingChange, index : Int64, term : Int64, reply : Channel(MembershipError?)
    private alias Event = Message | Propose | ChangeMembership | Transfer | GetStatus

    # True while this node is the leader and has committed an entry in its
    # term, i.e. it knows the latest committed ISR.
    getter serving = BoolChannel.new(false)
    # Notified (non-blocking, coalesced) whenever `leader_uri` changes.
    getter leader_changed = Channel(Nil).new(1)

    EVENT_QUEUE_SIZE = 256

    @events = Channel(Event).new(EVENT_QUEUE_SIZE)
    @pending = Array(Pending).new
    @pending_changes = Array(PendingChange).new
    @leader_uri : String? = nil
    @leader = false
    @committed_isr : Set(Int32)? = nil
    @membership : Membership? = nil
    @committed_membership : Membership? = nil
    @peer_node_ids = Hash(String, Int32).new
    @removed_callbacks = Array(Int32 ->).new
    @state_lock = Mutex.new
    @stopped = Channel(Nil).new
    @transport : Transport? = nil
    @logged_conflict : IdConflict? = nil
    @synced_peers = Set(String).new
    @configured : Set(String)
    @warned_config = false

    def initialize(@id : String, peers : Enumerable(String), @node_id : Int32, uri : String,
                   @storage : Storage, election_timeout : Time::Span, heartbeat_interval : Time::Span,
                   @tick = 20.milliseconds, bootstrap = false)
      @core = Core.new(@id, peers, @node_id, uri, election_timeout, heartbeat_interval,
        Time.instant, @storage.load, bootstrap: bootstrap)
      @configured = peers.to_set << @id
      @committed_isr = @core.committed_isr
      @membership = @core.latest_membership
      @committed_membership = @core.committed_membership
      @peer_node_ids = @core.peer_node_ids.dup
    end

    def run(transport : Transport) : Nil
      @transport = transport
      spawn(event_loop, name: "raft node")
    end

    # Called by the transport for every received message.
    def deliver(msg : Message) : Nil
      @events.send msg
    rescue Channel::ClosedError
    end

    def leader_uri : String?
      @state_lock.synchronize { @leader_uri }
    end

    def leader? : Bool
      @state_lock.synchronize { @leader }
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
    # voter or learner. True while there's no membership yet. A node whose id
    # hasn't been seen by raft yet isn't a member.
    def member?(node_id : Int32) : Bool
      return true if node_id == @node_id
      @state_lock.synchronize do
        return true unless m = @membership
        m.members.any? { |addr| @peer_node_ids[addr]? == node_id }
      end
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

    # Add a node as non-voting learner. Each of the membership changes blocks
    # until the change is committed and returns nil, or returns why it was
    # refused (or lost, see MembershipError::Lost).
    def add_learner(addr : String) : MembershipError?
      change MembershipChange::AddLearner, addr
    end

    # Make a learner that has caught up a voter.
    def promote(addr : String) : MembershipError?
      change MembershipChange::Promote, addr
    end

    def remove_member(addr : String) : MembershipError?
      change MembershipChange::Remove, addr
    end

    private def change(change : MembershipChange, addr : String) : MembershipError?
      reply = Channel(MembershipError?).new(1)
      @events.send ChangeMembership.new(change, addr, reply)
      await reply, MembershipError::Lost
    rescue Channel::ClosedError
      MembershipError::NotLeader
    end

    # Hand leadership over to a caught up in-sync voter, the chosen one or any,
    # so the cluster fails over without waiting for an election timeout.
    def transfer_leadership(target : String? = nil) : TransferResult
      reply = Channel(TransferResult).new(1)
      @events.send Transfer.new(target, reply)
      await reply, TransferResult::NotLeader
    rescue Channel::ClosedError
      TransferResult::NotLeader
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

    private def event_loop : Nil
      loop do
        select
        when event = @events.receive?
          break unless event
          handle(event)
          break unless handle_queued
        when timeout(@tick)
        end
        @core.tick(Time.instant)
        flush
      end
    ensure
      @pending.each &.reply.send(false)
      @pending.clear
      @pending_changes.each &.reply.send(MembershipError::Lost)
      @pending_changes.clear
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
      in Propose
        if index = @core.propose(event.isr, Time.instant)
          @pending << Pending.new(index, @core.term, event.reply)
        else
          event.reply.send false
        end
      in ChangeMembership
        case result = @core.propose_membership(event.change, event.addr, Time.instant)
        in Int64
          @pending_changes << PendingChange.new(result, @core.term, event.reply)
        in MembershipError
          event.reply.send result
        end
      in Transfer
        event.reply.send @core.transfer_leadership(event.target)
      in GetStatus
        event.reply.send build_status
      end
    end

    private def build_status : Status
      match_index = Hash(String, Int64).new
      caught_up = Set(String).new
      if @core.role.leader?
        @core.peers.each do |p|
          match_index[p] = @core.match_index(p)
          caught_up << p if @core.caught_up?(p)
        end
      end
      Status.new(@id, @node_id, @core.role, @core.term, @core.leader, @core.leader_uri,
        @core.latest_membership, @core.committed_membership, @core.peer_node_ids.dup, match_index,
        @core.last_index, caught_up, @core.committed_isr)
    end

    private def flush : Nil
      if @core.dirty?
        begin
          @storage.save(@core.hard_state)
        rescue ex
          Log.fatal(exception: ex) { "Could not persist raft state to #{@storage.path}" }
          exit 1
        end
        @core.persisted
      end
      if transport = @transport
        @core.take_outbox.each { |(to, msg)| transport.send(to, msg) }
      end
      resolve_pending
      resolve_pending_changes
      publish_state
      sync_transport
    end

    private def resolve_pending : Nil
      return if @pending.empty?
      @pending.reject! do |p|
        if p.term == @core.term && @core.commit_index >= p.index
          p.reply.send true
          true
        elsif p.term != @core.term || !@core.role.leader?
          p.reply.send false
          true
        else
          false
        end
      end
    end

    private def resolve_pending_changes : Nil
      return if @pending_changes.empty?
      @pending_changes.reject! do |p|
        if p.term == @core.term && @core.commit_index >= p.index
          p.reply.send nil
          true
        elsif p.term != @core.term || !@core.role.leader?
          p.reply.send MembershipError::Lost
          true
        else
          false
        end
      end
    end

    # Connect to every member, and to the leader even if we don't know yet
    # that it is one: a node joining with an empty log can only answer the
    # leader that way before it has learned the membership.
    private def sync_transport : Nil
      transport = @transport || return
      wanted = @core.peers.to_set
      @core.departing.each { |p| wanted << p }
      @core.leader.try { |l| wanted << l unless l == @id }
      return if wanted == @synced_peers
      @synced_peers = wanted
      transport.update_peers(wanted)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def publish_state : Nil
      if (conflict = @core.id_conflict) != @logged_conflict
        @logged_conflict = conflict
        if conflict
          own = conflict.holder == @core.id ? " and not campaigning" : ""
          Log.error do
            "#{conflict}: ignoring #{conflict.addr}#{own}, delete .clustering_id on the copied node, " \
            "or if a node changed address, update the peer list and restart"
          end
        else
          Log.info { "Clustering id conflict resolved" }
        end
      end
      uri = @core.leader_uri
      isr = @core.committed_isr
      leader = @core.role.leader?
      membership = @core.latest_membership
      committed = @core.committed_membership
      peer_ids = @core.peer_node_ids
      changed = false
      removed = Array(Int32).new
      callbacks = nil
      @state_lock.synchronize do
        changed = uri != @leader_uri || leader != @leader
        @leader_uri = uri
        @leader = leader
        @committed_isr = isr
        @membership = membership
        @peer_node_ids = peer_ids.dup if peer_ids != @peer_node_ids
        if committed != @committed_membership
          if (before = @committed_membership) && committed
            (before.members - committed.members).each do |addr|
              @peer_node_ids[addr]?.try { |id| removed << id }
            end
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
      warn_if_config_differs(committed)
      callbacks.try do |cbs|
        removed.each do |id|
          Log.info { "Node #{id.to_s(36)} was removed from the cluster" }
          cbs.each { |cb| spawn(name: "raft member removed") { cb.call(id) } }
        end
      end
      @serving.set(@core.serving_leader?)
    end

    private def warn_if_config_differs(membership : Membership?) : Nil
      return if @warned_config || membership.nil?
      @warned_config = true
      members = membership.members
      return if members == @configured
      Log.warn do
        "Configured peers (#{@configured.to_a.sort.join(", ")}) differ from the cluster membership " \
        "(voters: #{membership.voters.to_a.sort.join(", ")}; learners: #{membership.learners.to_a.sort.join(", ")}). " \
        "The membership in the raft log is used, peers is only a seed for joining nodes."
      end
    end
  end
end
