require "systemd"
require "./client"
require "./coordinator"
require "../http/metrics_server"

# Elects the leader and makes the other nodes follow it. The leader election
# and ISR storage are done by an etcd cluster (EtcdController) or by the nodes
# themselves with Raft (RaftController), chosen by `[clustering] backend`.
abstract class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  def self.create(config : Config) : Controller
    case config.clustering_backend
    in .etcd? then EtcdController.new(config)
    in .raft? then RaftController.new(config)
    end
  end

  getter id : Int32

  @repli_client : Client? = nil
  # Reports the replication client's metrics while following, see Launcher
  property metrics_server : HTTP::MetricsServer? = nil
  @stopped = false
  @stopping = false

  def initialize(@config : Config)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri_or_default
  end

  abstract def coordinator : Coordinator

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  abstract def run(&)

  abstract def stop

  # Called when a graceful shutdown starts, before client connections are
  # closed. Peers may be shutting down at the same time, so losing leadership
  # from here on is expected and not a reason to exit with an error.
  def stopping : Nil
    @stopping = true
  end

  # Each node in a cluster has an unique id, for tracking ISR
  private def clustering_id : Int32
    id_file_path = File.join(@config.data_dir, ".clustering_id")
    begin
      id = File.read(id_file_path).to_i(36)
    rescue File::NotFoundError
      id = rand(Int32::MAX)
      Dir.mkdir_p @config.data_dir
      File.write(id_file_path, id.to_s(36))
      Log.info { "Generated new clustering ID" }
    end
    id
  end

  # Replicate from the leader until this node becomes the leader itself.
  private def follow_leader
    follow_leader_changes
  rescue ex : Error
    Log.fatal { ex.message }
    exit 36 # 36 for CF (Cluster Follower)
  rescue ex : Socket::BindError
    Log.fatal { ex.message }
    exit 36 # 36 for CF (Cluster Follower)
  rescue ex
    Log.fatal(exception: ex) { "Unhandled exception while following leader" }
    exit 36 # 36 for CF (Cluster Follower)
  end

  private abstract def follow_leader_changes

  private def report_metrics_of(client : Client?) : Nil
    @metrics_server.try &.clustering_client = client
  end

  private def execute_shell_command(command : String, event : String)
    return if command.empty?

    Log.info { "Executing #{event} hook in background: #{command}" }

    spawn name: "#{event} hook" do
      status = Process.run(command, shell: true, output: Process::Redirect::Inherit, error: Process::Redirect::Inherit)
      if status.success?
        Log.info { "#{event} hook completed successfully" }
      else
        Log.warn { "#{event} hook failed with exit code #{status.exit_code}" }
      end
    rescue ex
      Log.error(exception: ex) { "Failed to execute #{event} hook" }
    end
  end

  class Error < Exception; end
end

require "./etcd_controller"
require "./raft_controller"
