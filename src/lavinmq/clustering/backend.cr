module LavinMQ
  # Which coordination backend clustering uses for leader election, storing the
  # in-sync replica set and sharing the replication secret.
  enum ClusteringBackend
    # External etcd cluster (default, the original implementation).
    Etcd
    # Embedded Raft (the `raft` shard); no external dependency. The replication
    # secret is read from `<data_dir>/.replication_secret`.
    Raft
  end
end
