module LavinMQ::Clustering
  # The ISR + replication-secret role of a clustering backend, used by
  # Clustering::Server. All methods are safe to call from any thread.
  module Coordinator
    # Commit the in-sync replica set. Returns true once it is durable in the
    # coordinator (etcd write fenced on leadership / raft entry committed under
    # our term), false if rejected (deposed leader, lost/overwritten raft
    # leadership) so the caller retries rather than acknowledging against a
    # stale ISR.
    abstract def update_isr(synced_node_ids : Enumerable(Int32)) : Bool
    abstract def password : String
  end
end
