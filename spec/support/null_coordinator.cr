require "../../src/lavinmq/clustering/coordinator"

class NullCoordinator < LavinMQ::Clustering::Coordinator
  def password : String
    "null-coordinator-secret"
  end
end
