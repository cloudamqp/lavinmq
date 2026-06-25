require "../../src/lavinmq/clustering/coordinator"

class NullCoordinator < LavinMQ::Clustering::Coordinator
  def update_isr(synced_node_ids : Set(Int32)) : Nil
  end

  def password : String
    "null-coordinator-secret"
  end
end
