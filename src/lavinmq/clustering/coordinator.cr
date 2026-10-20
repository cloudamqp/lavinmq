module LavinMQ::Clustering
  # What Clustering::Server uses to write ISR and read/write the shared
  # replication secret.
  #
  # All methods are safe to call from any thread.
  abstract class Coordinator
    # This node isn't the leader anymore, so it can't change the ISR. That's
    # final: the new leader decides the ISR from then on, so nothing waiting
    # for an ISR change may be acknowledged.
    class StaleLeadership < Exception
    end

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
    # from the cluster, until removed with #remove_member_removed_listener.
    def add_member_removed_listener(listener : Int32 ->) : Nil
    end

    def remove_member_removed_listener(listener : Int32 ->) : Nil
    end
  end
end
