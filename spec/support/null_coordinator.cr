require "../../src/lavinmq/clustering/coordinator"

class NullCoordinator
  include LavinMQ::Clustering::Coordinator

  def update_isr(synced_node_ids : Enumerable(Int32)) : Bool
    true
  end

  def password : String
    "null-coordinator-secret"
  end
end
