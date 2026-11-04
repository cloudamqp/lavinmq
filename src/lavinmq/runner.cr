module LavinMQ
  # Decides when this node serves clients: StandaloneRunner right away, a
  # clustering controller (Clustering::Controller) once it leads. The
  # Launcher only uses it through this, the defaults are for a node without
  # clustering.
  abstract class Runner
    # Yields once this node is to start serving. Blocks until #stop.
    abstract def run(&)

    abstract def stop

    # A graceful shutdown starts, before client connections are closed
    def stopping : Nil
    end

    # The replication server for when this node leads
    def new_replicator : Clustering::Server?
    end

    # The leader's host, which client connections are forwarded to while this
    # node doesn't serve, nil when there's none to forward to
    def leader_host : String?
    end

    # Writes the runner's own metrics to every Prometheus scrape, whatever the
    # node's role, see HTTP::MetricsServer
    def write_metrics(writer) : Nil
    end
  end
end
