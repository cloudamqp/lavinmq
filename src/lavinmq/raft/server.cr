require "raft"
require "../bool_channel"
require "./cluster_command"
require "./cluster_state"
require "./cluster_state_machine"

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
    @on_leader_change : Proc(UInt64?, Nil)?
    @last_observed_leader_id : UInt64? = nil

    getter node_id : UInt64
    getter state_machine : ClusterStateMachine
    getter is_leader : BoolChannel

    def initialize(
      @data_dir : String,
      @advertised_address : String,
      @transport : ::Raft::Transport,
      @execution_context : Fiber::ExecutionContext = Fiber::ExecutionContext::Concurrent.new("raft"),
    )
      @node_id = load_or_generate_node_id
      @state_machine = ClusterStateMachine.new
      @is_leader = BoolChannel.new(false)
      config = ::Raft::Config.new
      config.data_dir = File.join(@data_dir, "raft")
      Dir.mkdir_p(config.data_dir)
      @node = ::Raft::Node(ClusterCommand).new(
        id: @node_id,
        peers: [] of ::Raft::NodeID,
        config: config,
        state_machine: @state_machine,
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
      @is_leader.close
    end

    def bootstrap : Bool
      run_on_tick { @node.bootstrap }
    end

    # Adds a node as a learner. raft.cr auto-promotes it to a voter once it has
    # caught up with the leader's log (see Node#maybe_promote_learner), so no
    # explicit promotion call is needed.
    def add_server(node_id : UInt64, address : String) : Bool
      run_on_tick { @node.add_server(node_id, address) }
    end

    def propose(cmd : ClusterCommand) : Bool
      run_on_tick { @node.propose(cmd) }
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

    def isr : Set(UInt64)
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
        current_leader = @node.leader_id
        unless current_leader == @last_observed_leader_id
          @last_observed_leader_id = current_leader
          @on_leader_change.try &.call(current_leader)
        end
      end
    rescue ::Channel::ClosedError
    ensure
      @node.close
      @tick_done.close
    end

    private def wire_callbacks : Nil
      @node.on_role_change do |_old_role, new_role|
        @is_leader.set(new_role == ::Raft::Role::Leader)
      end
      @node.on_configuration_applied do |peers|
        peers.each do |peer|
          next if peer.id == @node_id || peer.address.empty?
          raft_addr, _, _data_addr = peer.address.partition(",")
          host, _, port = raft_addr.rpartition(":")
          next if port.empty?
          @transport.register_peer(peer.id, host, port.to_i)
        end
      end
    end

    private def load_or_generate_node_id : UInt64
      Dir.mkdir_p(@data_dir)
      raft_path = File.join(@data_dir, ".raft_node_id")
      legacy_path = File.join(@data_dir, ".clustering_id")

      if File.exists?(raft_path)
        File.read(raft_path).strip.to_u64
      elsif File.exists?(legacy_path)
        raw = File.read(legacy_path).strip
        legacy_int32 = raw.to_i32(36)
        id = legacy_int32.to_u64
        File.write(raft_path, id.to_s)
        Log.info { "Migrated clustering id: base-36 #{raw} (decimal #{id}) → .raft_node_id" }
        id
      else
        id = Random::Secure.rand(UInt64)
        File.write(raft_path, id.to_s)
        Log.info { "Generated new raft node id #{id}" }
        id
      end
    end
  end
end
