require "./coordinator"
require "./raft/node"

module LavinMQ::Clustering
  class RaftCoordinator < Coordinator
    class StaleLeadership < Exception
      def initialize
        super("Not the leader, ISR not updated")
      end
    end

    def initialize(@node : Raft::Node, @password : String)
    end

    # Blocks until a majority has the new ISR. Raises when leadership is lost
    # first, the new leader decides the ISR from then on.
    def update_isr(synced_node_ids : Set(Int32)) : Nil
      raise StaleLeadership.new unless @node.propose_isr(synced_node_ids)
    end

    def password : String
      @password
    end

    def member?(node_id : Int32) : Bool
      @node.member?(node_id)
    end

    def on_member_removed(&block : Int32 ->) : Nil
      @node.on_member_removed(&block)
    end
  end
end
