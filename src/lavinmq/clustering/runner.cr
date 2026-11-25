require "systemd"
require "../filesystem"
require "./client"
require "./coordinator"
require "./server"
require "../runner"

# Elects the leader and makes the other nodes follow it. Subclasses do the
# leader election and ISR storage, e.g. with an etcd cluster (EtcdRunner).
abstract class LavinMQ::Clustering::Runner < LavinMQ::Runner
  Log = LavinMQ::Log.for "clustering.runner"

  def self.create(config : Config) : Runner
    EtcdRunner.new(config)
  end

  getter id : Int32

  @repli_client = Atomic(Client?).new(nil)
  # Set by #stopping and #stop from the shutdown fiber, read by #run's and
  # the HTTP API's fibers
  @stopping = Atomic(Bool).new(false)

  # *id*: this node's unique id, for tracking the ISR, e.g. the one in
  # .clustering_id, see .clustering_id
  def initialize(@config : Config, @id : Int32)
    @advertised_uri = Runner.advertised_uri(@config)
    Log.info { "ID: #{@id.to_s(36)}" }
  end

  abstract def coordinator : Coordinator

  # The replication server of the term this node leads, from #run's fiber
  # until it's closed, see #close
  @replicator = Atomic(Server?).new(nil)

  # The replication server for a term this node leads, which #run yields to
  # the broker. See #listen and #close.
  private def new_replicator : Server
    Server.new(@config, coordinator, @id).tap { |r| @replicator.set(r) }
  end

  # Accepts followers once the broker has loaded, unless stopped meanwhile
  private def listen(replicator : Server) : Nil
    return if @stopping.get
    bind = @config.clustering_bind || return
    listener = begin
      TCPServer.new(bind, @config.clustering_port)
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end
    spawn(name: "Clustering listener") { replicator.listen(listener) }
  end

  # Disconnects the followers and stores the checksums, after the broker's
  # last writes
  def close : Nil
    @replicator.swap(nil).try &.close
  end

  # The URI this node campaigns with, which followers read to find the leader:
  # its replication address, and in the query the client ports it listens on,
  # so followers forward to them even when their own ports differ. Ports given
  # in the configured advertised_uri's query win, for when the leader is reached
  # through other ports than it binds (NAT, container port mappings).
  def self.advertised_uri(config : Config) : String
    uri = URI.parse(config.clustering_advertised_uri ||
                    "tcp://#{System.hostname}:#{config.clustering_port}")
    params = uri.query_params
    tls = config.tls_configured?
    {
      "amqp"  => config.amqp_port,
      "amqps" => tls ? config.amqps_port : 0,
      "mqtt"  => config.mqtt_port,
      "mqtts" => tls ? config.mqtts_port : 0,
      "http"  => config.http_port,
      "https" => tls ? config.https_port : 0,
    }.each do |protocol, port|
      params[protocol] = port.to_s if port > 0 && !params.has_key?(protocol)
    end
    uri.query_params = params
    uri.to_s
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the runner's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  abstract def run(& : Replicator? -> _)

  # A graceful shutdown starts, before client connections are closed, see
  # #shutdown. Peers may be shutting down at the same time, so losing
  # leadership from here on is expected and not a reason to exit with an error.
  private def stopping : Nil
    @stopping.set(true)
  end

  # Two nodes advertising the same replication address are the same node, even
  # if they advertise different client ports
  private def same_node?(a : String, b : String) : Bool
    a = URI.parse(a)
    b = URI.parse(b)
    a.hostname == b.hostname && (a.port || 5679) == (b.port || 5679)
  end

  # The id in .clustering_id, generated if there's none
  protected def self.clustering_id(config : Config) : Int32
    stored_clustering_id(config) || begin
      id = rand(Int32::MAX)
      FileSystem.mkdir_p config.data_dir
      FileSystem.replace(File.join(config.data_dir, ".clustering_id"), &.print(id.to_s(36)))
      Log.info { "Generated new clustering ID" }
      id
    end
  end

  # The id in .clustering_id, nil if there's none
  protected def self.stored_clustering_id(config : Config) : Int32?
    File.read(File.join(config.data_dir, ".clustering_id")).to_i(36)
  rescue File::NotFoundError
    nil
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

  # The replication client's while following
  def write_metrics(writer) : Nil
    @repli_client.get.try &.write_metrics(writer)
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

require "./etcd_runner"
