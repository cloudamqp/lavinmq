require "http/server/handler"

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

    # The raft cluster, for its HTTP API and metrics
    def raft : Clustering::RaftController?
    end

    # Serves the broker's HTTP API, or nothing when nil, on a lavinmqctl
    # socket it owns for the process. False when it owns none, the broker's
    # HTTP server binds the socket itself then.
    def serve_control_api(handler : ::HTTP::Handler?) : Bool
      false
    end

    # The path of the lavinmqctl socket it owns
    def control_path : String?
    end

    # Where clustering metrics are reported, see HTTP::MetricsServer
    def metrics_server=(server : HTTP::MetricsServer?)
    end
  end
end
