module LavinMQ
  module Clustering
    # A node's participation in cluster leader election. The Launcher holds
    # exactly one Elector and drives the process lifecycle through it:
    #
    #   elector.campaign do
    #     # elected — start serving; the block returns once the data plane
    #     # is up, while campaign keeps blocking for as long as leadership
    #     # is held
    #   end
    #   # leadership lost => the elector exits the process (systemd restarts
    #   # the node as a follower); clean stop => campaign returns normally
    #
    # Implementations: EtcdBackend, Raft::Backend, StandaloneBackend.
    module Elector
      # Participate in leader election; blocks for the lifetime of the
      # process. Yields exactly once, when this node has been elected and
      # may serve.
      abstract def campaign(& : -> Nil)

      # Withdraw from the election / shut down. Called from the signal path.
      abstract def stop
    end
  end
end
