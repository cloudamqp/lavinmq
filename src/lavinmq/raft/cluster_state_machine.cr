require "raft"
require "./cluster_command"

module LavinMQ::Raft
  class ClusterStateMachine < ::Raft::StateMachine(ClusterCommand)
    SNAPSHOT_VERSION = 1_u8

    getter secret : String = ""
    getter isr : Set(UInt64) = Set(UInt64).new

    def apply(entry : ClusterCommand)
      case entry
      in ClusterCommand::SetSecret     then @secret = entry.secret
      in ClusterCommand::AddToIsr      then @isr.add(entry.node_id)
      in ClusterCommand::RemoveFromIsr then @isr.delete(entry.node_id)
      end
    end

    def snapshot(io : IO) : Nil
      raise NotImplementedError.new("snapshot not yet implemented")
    end

    def restore(io : IO) : Nil
      raise NotImplementedError.new("restore not yet implemented")
    end
  end
end
