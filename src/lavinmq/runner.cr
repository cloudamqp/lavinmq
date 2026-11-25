require "./clustering/replicator"

module LavinMQ
  # Decides when this node serves clients: StandaloneRunner right away, a
  # clustering runner (Clustering::Runner) once it leads. The
  # Launcher only uses it through this, the defaults are for a node without
  # clustering.
  abstract class Runner
    # Yields once this node is to start serving, with what the broker
    # replicates through, nil without clustering. Blocks until #shutdown.
    abstract def run(& : Clustering::Replicator? -> _)

    # Shuts down gracefully. The block closes what serves clients, e.g. the
    # listeners and the broker, while this node still has its role, so what
    # closing writes is still replicated, and losing leadership from the
    # block on is expected. Then the node gives up its role and #run returns.
    def shutdown(& : -> _) : Nil
      stopping
      begin
        yield
      ensure
        stop
      end
    end

    # A graceful shutdown starts, see #shutdown
    private def stopping : Nil
    end

    # Gives up the node's role, so #run returns, see #shutdown
    private abstract def stop

    # Called once the Launcher is done with the broker, before the process
    # exits, so what the runner holds for it can go, e.g. replication: its
    # checksums are stored after the broker's last writes
    def close : Nil
    end

    # Writes the runner's own metrics to every Prometheus scrape, whatever the
    # node's role, see HTTP::MetricsServer
    def write_metrics(writer) : Nil
    end
  end
end
