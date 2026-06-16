module LavinMQ::Clustering
  # The ISR + replication-secret role of a clustering backend, used by
  # Clustering::Server. All methods are safe to call from any thread.
  module Coordinator
    abstract def update_isr(synced_node_ids : Enumerable(Int32)) : Nil
    abstract def password : String
  end
end
