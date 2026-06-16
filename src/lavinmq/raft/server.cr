require "raft"
require "../bool_channel"
require "./cluster_command"
require "./cluster_state"
require "./cluster_state_machine"
require "./peer_address"

module LavinMQ::Raft
  class Server
    Log = LavinMQ::Log.for "raft.server"

    GROUP_ID = 0_u64

    @started = false
    @stop_signal = ::Channel(Nil).new
    @tick_done = ::Channel(Nil).new
    @stopping = false
    # Backpressure buffer; run_on_tick callers block when full. Tick fiber drains.
    @actions = ::Channel({-> Bool, ::Channel(Bool)}).new(64)
    # {log index, term proposed under, reply}. Tick-fiber-only. Resolved each
    # tick iteration: committed-under-our-term => true, overwritten/lost => false.
    @commit_waiters = [] of Tuple(UInt64, UInt64, ::Channel(Bool))
    # Parsed peer addresses, rebuilt once per configuration change (on the tick
    # fiber) and read cross-fiber by the follow-leader path. Replaced wholesale
    # under the lock so readers always see a consistent snapshot.
    @peer_addresses_lock = Mutex.new
    @peer_addresses = {} of UInt64 => PeerAddress
    @on_leader_change : Proc(UInt64?, Nil)?
    @last_observed_leader_id : UInt64? = nil

    getter node_id : Int32
    getter state_machine : ClusterStateMachine
    getter is_leader : BoolChannel

    def initialize(
      @data_dir : String,
      @advertised_address : String,
      @transport : ::Raft::Transport,
      @execution_context : Fiber::ExecutionContext = Fiber::ExecutionContext::Concurrent.new("raft"),
    )
      @node_id = load_or_generate_node_id
      metrics = ::Raft::Metrics.new(node_id: @node_id.to_u64, group_id: GROUP_ID)
      @state_machine = ClusterStateMachine.new
      @is_leader = BoolChannel.new(false)
      config = ::Raft::Config.new
      config.data_dir = File.join(@data_dir, "raft")
      Dir.mkdir_p(config.data_dir)
      @node = ::Raft::Node(ClusterCommand).new(
        id: @node_id.to_u64,
        peers: [] of ::Raft::NodeID,
        config: config,
        state_machine: @state_machine,
        metrics: metrics,
        group_id: GROUP_ID,
        address: @advertised_address,
      )
      @transport.register_channel(GROUP_ID, @node.inbox)
      wire_callbacks
    end

    def start : Nil
      return if @started
      @started = true
      @execution_context.spawn(name: "Raft::Server#tick_loop") { tick_loop }
    end

    def stop : Nil
      return if @stopping
      @stopping = true
      @stop_signal.close
      @tick_done.receive? if @started
      @actions.close
      while item = @actions.receive? # drain buffered actions: reply false so callers unblock
        item[1].send(false) rescue nil
      end
      @is_leader.close
    end

    def bootstrap : Bool
      run_on_tick { @node.bootstrap }
    end

    # Adds a node as a learner. raft.cr auto-promotes it to a voter once it has
    # caught up with the leader's log (see Node#maybe_promote_learner), so no
    # explicit promotion call is needed.
    def add_server(node_id : Int32, address : String) : Bool
      run_on_tick { @node.add_server(node_id.to_u64, address) }
    end

    def propose(cmd : ClusterCommand) : Bool
      run_on_tick { @node.propose(cmd) }
    end

    # Propose a command and block until it is confirmed committed under the
    # term it was proposed in. Returns false if we are not leader, lose
    # leadership before it commits, the slot is overwritten by a new leader,
    # or the server is stopping. Use this when the write must be durable
    # (e.g. ISR updates); use `propose` when local append is enough.
    def propose_committed(cmd : ClusterCommand) : Bool
      committed = ::Channel(Bool).new(1)
      appended =
        begin
          run_on_tick do
            if @node.propose(cmd)
              @commit_waiters << {@node.log.last_index, @node.current_term, committed}
              true
            else
              false
            end
          end
        rescue # run_on_tick raises "stopping" if stop() began; treat as not-committed
          false
        end
      return false unless appended
      select
      when ok = committed.receive
        ok
      when @is_leader.when_false.receive? # lost leadership (nil-safe if closed at shutdown)
        false
      end
    end

    # Hand raft leadership to `target`. Returns false when this node is not
    # leader, the target is not a known voter, or the target is this node.
    def transfer_leadership(to target : Int32) : Bool
      run_on_tick { @node.transfer_leadership(to: target.to_u64) }
    end

    # Marshal a mutating Node call onto the tick fiber's thread.
    # NOTE: callbacks fired by the tick fiber (on_role_change,
    # on_configuration_applied) MUST NOT call back into this method — the tick
    # fiber would be blocked waiting on its own actions channel. Deadlock.
    private def run_on_tick(&block : -> Bool) : Bool
      raise "Raft::Server not started" unless @started
      raise "Raft::Server stopping" if @stopping
      reply = ::Channel(Bool).new(1)
      begin
        @actions.send({block, reply})
      rescue ::Channel::ClosedError
        raise "Raft::Server stopping"
      end
      reply.receive
    end

    def leader_id : UInt64?
      @node.leader_id
    end

    def on_leader_change(&block : UInt64? ->) : Nil
      @on_leader_change = block
    end

    # The underlying Raft node. Exposed so the HTTP admin handler can
    # interact with it directly. Not stable public API; treat as developer
    # surface only.
    def node : ::Raft::Node(ClusterCommand)
      @node
    end

    # Peers as seen by the local Node. Reading from outside the tick fiber is
    # an inconsistency-prone race — only the tick fiber mutates @node.peers,
    # but readers may observe a stale snapshot. Used by the Runner for
    # best-effort follow-leader lookups; not authoritative.
    def peers : Array(::Raft::Peer)
      @node.peers
    end

    # The node ids currently in the voting set (learners excluded).
    def voters : Array(UInt64)
      @node.voters.map(&.id)
    end

    def state : ClusterState
      @state_machine.state
    end

    def secret : String
      @state_machine.secret
    end

    def isr : Set(Int32)
      @state_machine.isr
    end

    private def tick_loop : Nil
      # FIXME: `timeout(50.ms)` in `select` only fires when no other case is
      # ready. Under sustained inbox/actions load (e.g. a future per-queue
      # data-plane Node), @node.tick would be starved and the node would miss
      # heartbeats / election timeouts. For the current low-frequency
      # metadata inbox this is harmless. When a high-frequency Raft node
      # lands, replace this with a dedicated tick-timer fiber sending on a
      # buffered(1) channel that competes fairly with the other select cases.
      loop do
        select
        when msg = @node.inbox.receive
          @node.step(msg)
        when item = @actions.receive
          action, reply = item
          begin
            reply.send(action.call)
          rescue ex
            # Don't crash the tick fiber on an action's exception; surface false
            # to the caller (semantically "didn't happen") and log.
            reply.send(false)
            Log.error(exception: ex) { "action raised in tick loop: #{ex.message}" }
          end
        when @stop_signal.receive?
          break
        when timeout(50.milliseconds)
          @node.tick
        end
        @node.take_messages.each do |target_id, outbound|
          @transport.outbox.send({target_id, outbound})
        end
        resolve_commit_waiters
        current_leader = @node.leader_id
        unless current_leader == @last_observed_leader_id
          @last_observed_leader_id = current_leader
          @on_leader_change.try &.call(current_leader)
        end
      end
    rescue ::Channel::ClosedError
    ensure
      @commit_waiters.each { |_index, _term, ch| ch.send(false) rescue nil }
      @commit_waiters.clear
      @node.close
      @tick_done.close
    end

    # Runs on the tick fiber. Lost leadership => fail all (can't commit our
    # proposals). Else resolve any whose index has applied: true iff the slot
    # still holds the term we proposed under (else a newer leader overwrote it).
    private def resolve_commit_waiters : Nil
      return if @commit_waiters.empty?
      leader = @node.role.leader?
      applied = @node.last_applied
      @commit_waiters.reject! do |index, term, ch|
        if !leader
          ch.send(false) rescue nil
          true
        elsif applied >= index
          ch.send(@node.log.term_at(index) == term) rescue nil
          true
        else
          false
        end
      end
    end

    private def wire_callbacks : Nil
      @node.on_role_change do |_old_role, new_role|
        @is_leader.set(new_role.leader?)
      end
      @node.on_configuration_applied do |peers|
        # Parse each peer's address once here (membership changes are rare),
        # register the transport route, and cache the parsed form for the
        # follow-leader path to reuse.
        addresses = {} of UInt64 => PeerAddress
        peers.each do |peer|
          next if peer.id == @node_id || peer.address.empty?
          addr = PeerAddress.parse?(peer.address)
          next if addr.nil?
          addresses[peer.id] = addr
          host, port = addr.raft_endpoint
          @transport.register_peer(peer.id, host, port)
        end
        @peer_addresses_lock.synchronize { @peer_addresses = addresses }
      end
    end

    # The parsed address of a configured peer, or nil if unknown. Reflects the
    # last applied configuration. Safe to call from any fiber.
    def peer_address(node_id : UInt64) : PeerAddress?
      @peer_addresses_lock.synchronize { @peer_addresses[node_id]? }
    end

    private def load_or_generate_node_id : Int32
      Dir.mkdir_p(@data_dir)
      path = File.join(@data_dir, ".clustering_id")
      begin
        File.read(path).strip.to_i32(36)
      rescue File::NotFoundError
        id = Random::Secure.rand(Int32::MAX)
        File.write(path, id.to_s(36))
        Log.info { "Generated new clustering id #{id}" }
        id
      end
    end
  end
end
