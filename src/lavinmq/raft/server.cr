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
    @actions = ::Channel({-> Bool, ::Channel(Bool)}).new(64)

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

    private def run_on_tick(&block : -> Bool) : Bool
      raise "Raft::Server not started" unless @started
      raise "Raft::Server stopping" if @stopping
      reply = ::Channel(Bool).new(1)
      @actions.send({block, reply})
      reply.receive
    end

    def leader_id : UInt64?
      @node.leader_id
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
          reply.send(action.call)
        when @stop_signal.receive?
          break
        when timeout(50.milliseconds)
          @node.tick
        end
        @node.take_messages.each do |target_id, outbound|
          @transport.outbox.send({target_id, outbound})
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
      path = File.join(@data_dir, ".clustering_id")
      begin
        # A non-decimal file (e.g. the legacy base-36 Int32 written by the etcd
        # controller) raises ArgumentError here — intentional fail-loud. The
        # launcher-integration slice must migrate existing ids before this runs.
        File.read(path).strip.to_u64
      rescue File::NotFoundError
        id = Random::Secure.rand(UInt64)
        File.write(path, id.to_s)
        Log.info { "Generated new clustering id #{id}" }
        id
      end
    end
  end
end
