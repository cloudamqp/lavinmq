require "systemd"
require "../filesystem"
require "./client"
require "./coordinator"
require "./server"
require "../runner"

# Elects the leader and makes the other nodes follow it. The leader election
# and ISR storage are done by an etcd cluster (EtcdRunner) or by the nodes
# themselves with Raft (RaftRunner), chosen by `[clustering] backend`.
abstract class LavinMQ::Clustering::Runner < LavinMQ::Runner
  Log = LavinMQ::Log.for "clustering.runner"

  def self.create(config : Config) : Runner
    case config.clustering_backend
    in .etcd? then EtcdRunner.new(config)
    in .raft? then RaftRunner.new(config)
    end
  end

  getter id : Int32

  @repli_client = Atomic(Client?).new(nil)
  # Set by #stopping and #stop from the shutdown fiber, read by #run's and
  # the HTTP API's fibers
  @stopping = Atomic(Bool).new(false)
  # Stops serving as the leader, see #on_demote
  @demote : (Proc(Nil)? ->)? = nil
  # See #on_leader_change, set before #run
  @leader_change : Proc(Nil)? = nil

  # How long stopping to serve, or a startup that lost leadership, may take
  # before the process exits instead, so a node that isn't the leader anymore
  # never keeps serving
  property demotion_timeout : Time::Span = 60.seconds

  # *id*: this node's unique id, for tracking the ISR. The etcd backend keeps
  # it in .clustering_id, see .clustering_id, the raft backend in its raft
  # state, see RaftRunner.
  def initialize(@config : Config, @id : Int32)
    @advertised_uri = Runner.advertised_uri(@config)
    Log.info { "ID: #{@id.to_s(36)}" }
  end

  abstract def coordinator : Coordinator

  # The replication server of the term this node leads, from #run's fiber
  # until #broker_closed
  @replicator = Atomic(Server?).new(nil)

  # The replication server for a term this node leads, which #run yields to
  # the broker. See #listen and #broker_closed.
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
  # last writes. The Launcher calls it after #run returns, and when it stops
  # serving, see #on_demote.
  def broker_closed : Nil
    @replicator.swap(nil).try &.close
  end

  # The URI this node campaigns with, which followers read to find the leader:
  # its replication address, and in the query the client ports it listens on,
  # so followers forward to them even when their own ports differ. Ports given
  # in the configured advertised_uri's query win, for when the leader is reached
  # through other ports than it binds (NAT, container port mappings).
  def self.advertised_uri(config : Config) : String
    uri = URI.parse(config.clustering_advertised_uri_or_default)
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

  abstract def stop

  # Called when a graceful shutdown starts, before client connections are
  # closed. Peers may be shutting down at the same time, so losing leadership
  # from here on is expected and not a reason to exit with an error.
  def stopping : Nil
    @stopping.set(true)
  end

  # Registers how the Launcher stops serving when this node stops being the
  # leader, without exiting: this node then follows the new leader. When
  # leadership is handed over the block gets the handover to call once
  # clients are disconnected, while the followers are still connected (the
  # target has to stay in the ISR). When it was lost, it gets nil. Once the
  # broker is closed it calls #broker_closed.
  def on_demote(&block : Proc(Nil)? ->) : Nil
    @demote = block
  end

  # Exits unless *done* is closed within the demotion timeout, counted from
  # when *armed* fires, or right away without it: a node that isn't the
  # leader anymore must never keep serving.
  private def watchdog(done : Channel(Nil), message : String, armed : Channel? = nil) : Nil
    if armed
      select
      when done.receive?
        return
      when armed.receive?
      end
    end
    select
    when done.receive?
    when timeout(@demotion_timeout)
      Log.fatal { message }
      exit 3
    end
  end

  # Stops serving, see #on_demote. Exits if that fails or hangs.
  private def demote(hand_over : Proc(Nil)?) : Nil
    done = Channel(Nil).new
    spawn(watchdog(done, "Stopping to serve took longer than #{@demotion_timeout.total_seconds.to_i}s, exiting"),
      name: "Demotion watchdog")
    begin
      # Nothing more is confirmed. Closed once the broker is, see
      # #broker_closed, it writes files until then.
      @replicator.get.try &.fence unless hand_over
      if demote = @demote
        demote.call(hand_over)
      else
        hand_over.try &.call
        broker_closed
      end
    rescue ex
      Log.fatal(exception: ex) { "Failed to stop serving, exiting" }
      exit 3
    ensure
      done.close
    end
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

  # Runs a follower monitor, which replicates from the leader until this node
  # becomes the leader itself
  private def follow_leader(&)
    yield
  rescue ex : Error
    Log.fatal { ex.message }
    exit 36 # 36 for CF (Cluster Follower)
  rescue ex
    Log.fatal(exception: ex) { "Unhandled exception while following leader" }
    exit 36 # 36 for CF (Cluster Follower)
  end

  # The leader it replicates from, which client connections are forwarded to
  def leader : Client::Leader?
    @repli_client.get.try &.leader
  end

  def on_leader_change(&block : ->) : Nil
    @leader_change = block
  end

  # After a change of the replication client, or of its leader host
  private def leader_changed : Nil
    @leader_change.try &.call
  end

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
require "./raft_runner"
