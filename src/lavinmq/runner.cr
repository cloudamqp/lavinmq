require "./clustering/replicator"

module LavinMQ
  # Decides when this node serves clients: StandaloneRunner right away, a
  # clustering runner (Clustering::Runner) whenever it leads. The
  # Launcher only uses it through this, the defaults are for a node without
  # clustering.
  abstract class Runner
    # Yields whenever this node is to start serving, with what the broker
    # replicates through, nil without clustering. Blocks until #stop.
    abstract def run(& : Clustering::Replicator? -> _)

    abstract def stop

    # A graceful shutdown starts, before client connections are closed
    def stopping : Nil
    end

    # How the Launcher stops serving when this node stops leading, while the
    # process keeps running, see Clustering::Runner#on_demote
    def on_demote(&_block : Proc(Nil)? ->) : Nil
    end

    # The broker it last yielded for is closed, so what the runner holds for
    # it can go, e.g. replication: its checksums are stored after the
    # broker's last writes
    def broker_closed : Nil
    end

    # The cluster's membership and leadership, for the HTTP API, nil when
    # the runner has none to manage
    def cluster_admin : Clustering::ClusterAdmin?
    end

    # The leader, which client connections are forwarded to while this node
    # doesn't serve, nil when there's none to forward to
    def leader : Clustering::Client::Leader?
    end

    # Registers what to call after what #leader returns may have changed
    def on_leader_change(&_block : ->) : Nil
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
