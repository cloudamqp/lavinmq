require "raft"
require "../bool_channel"
require "./cluster_command"
require "./cluster_state_machine"

module LavinMQ::Raft
  class Server
    Log = LavinMQ::Log.for "raft.server"

    GROUP_ID = 0_u64

    @started = false

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
      @node.close
      @is_leader.close
    end

    def bootstrap : Bool
      @node.bootstrap
    end

    def add_server(node_id : UInt64, address : String) : Bool
      @node.add_server(node_id, address)
    end

    def promote_learner(node_id : UInt64) : Bool
      @node.promote_learner(node_id)
    end

    def propose(cmd : ClusterCommand) : Bool
      @node.propose(cmd)
    end

    def leader_id : UInt64?
      @node.leader_id
    end

    private def tick_loop : Nil
      loop do
        select
        when msg = @node.inbox.receive
          @node.step(msg)
        when timeout(50.milliseconds)
          @node.tick
        end
        @node.take_messages.each do |target_id, outbound|
          @transport.outbox.send({target_id, outbound})
        end
      end
    rescue ::Channel::ClosedError
      # inbox closed by stop — exit cleanly
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
