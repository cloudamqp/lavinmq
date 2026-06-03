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
  end
end
