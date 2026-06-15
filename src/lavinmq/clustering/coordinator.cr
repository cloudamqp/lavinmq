module LavinMQ::Clustering
  # Supplies Clustering::Server with the shared replication secret. Under
  # Viewstamped Replication there is no external ISR store — failover safety
  # comes from the view-change log-selection rule and majority-quorum commit —
  # so the coordinator's only remaining job is the secret.
  #
  # All methods are safe to call from any thread.
  abstract class Coordinator
    # The cluster's shared replication secret.
    abstract def password : String
  end
end
