module LavinMQ::Clustering
  # What Clustering::Server uses to write ISR and read/write the shared
  # replication secret.
  #
  # All methods are safe to call from any thread.
  abstract class Coordinator
    # Replace the ISR set wholesale with the given node ids.
    abstract def update_isr(synced_node_ids : Set(Int32)) : Nil

    # Read the cluster's shared replication secret, generating one if missing.
    abstract def password : String

    # Whether the node with this clustering id belongs to the cluster. Only
    # members may replicate from the leader and be in the ISR.
    def member?(node_id : Int32) : Bool
      true
    end

    # Called, from a fiber of its own, with the id of a node that was removed
    # from the cluster.
    def on_member_removed(&_block : Int32 ->) : Nil
    end
  end
end
