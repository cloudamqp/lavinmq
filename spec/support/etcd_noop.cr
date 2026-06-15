# etcd has been replaced by the Viewstamped Replication coordinator, so the old
# etcd test harness is gone. These no-ops keep specs that still carry the
# `add_etcd_around_each` / `with_etcd` scaffolding compiling and running — their
# clustering exercises use the in-process leader+follower helper (with_clustering
# in clustering_helper.cr), which never needed etcd.

# Was: install an around_each hook that starts/stops a real etcd. Now a no-op.
def add_etcd_around_each
end

# Was: run the block with a real etcd available. Now just runs the block.
def with_etcd(&)
  yield
end
