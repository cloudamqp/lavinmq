module LavinMQ
  # Decides when this node serves clients: StandaloneRunner right away, a
  # clustering controller (Clustering::Controller) whenever it leads. The
  # Launcher only uses it through this, the defaults are for a node without
  # clustering.
  abstract class Runner
    # Yields whenever this node is to start serving. Blocks until #stop.
    abstract def run(&)

    abstract def stop

    # A graceful shutdown starts, before client connections are closed
    def stopping : Nil
    end

    # How the Launcher stops serving when this node stops leading, while the
    # process keeps running, see Clustering::RaftController#on_demote.
    def on_demote(&_block : Proc(Nil)? ->) : Nil
    end

    # The replication server for a term this node leads
    def new_replicator : Clustering::Server?
    end

    # The raft cluster, for its HTTP API
    def raft : Clustering::RaftController?
    end

    # The leader's host, which client connections are forwarded to while this
    # node doesn't serve, nil when there's none to forward to
    def leader_host : String?
    end

    # This node's view of the cluster, which the lavinmqctl socket answers
    # GET /api/cluster with while this node doesn't serve, see
    # HTTP::ControlSocket. Nil to answer as a follower.
    def cluster_status : String?
    end

    # Writes the runner's own metrics to every Prometheus scrape, whatever the
    # node's role, see HTTP::MetricsServer
    def write_metrics(writer) : Nil
    end
  end
end
