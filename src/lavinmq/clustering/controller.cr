require "systemd"
require "./client"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  @repli_client : Client? = nil
  @transport : Raft::TCPTransport? = nil

  def initialize(@config : Config)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
    @node = Raft::Node.new(@config.clustering_raft_address, @config.clustering_peer_addresses,
      @id, @advertised_uri, Raft::Storage.new(@config.data_dir),
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      bootstrap: may_bootstrap?)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  def run(&)
    start_node
    spawn(follow_leader, name: "Follower monitor")
    select
    when @node.serving.when_true.receive
    when @stop_signal.receive?
      return
    end
    return if @stopped
    ensure_in_isr!
    @repli_client.try &.close
    # No follower is replicating from this node yet, so none of them can be
    # trusted to have what it's about to confirm. They rejoin the ISR as they
    # finish syncing.
    @coordinator.update_isr(Set{@id})
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    yield
    @node.serving.when_false.receive
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopping
      Log.fatal { "Lost leadership" }
      exit 3
    end
  rescue RaftCoordinator::StaleLeadership
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopping
      Log.fatal { "Lost leadership before starting to serve" }
      exit 3
    end
  end

  @stopped = false
  @stopping = false
  @stop_signal = Channel(Nil).new

  # Called when a graceful shutdown starts, before client connections are
  # closed. Peers may be shutting down at the same time, so losing leadership
  # from here on is expected and not a reason to exit with an error.
  def stopping : Nil
    @stopping = true
  end

  def stop
    return if @stopped
    @stopped = @stopping = true
    @stop_signal.close
    @repli_client.try &.close
    hand_over_leadership
    @node.close
  end

  # Lets an in-sync follower take over right away instead of after an
  # election timeout.
  private def hand_over_leadership : Nil
    return unless leader?
    return unless @node.transfer_leadership
    deadline = Time.instant + @config.clustering_election_timeout.milliseconds * 2
    while @node.leader? && Time.instant < deadline
      select
      when @node.leader_changed.receive
      when timeout(deadline - Time.instant)
      end
    end
  end

  private def start_node : Nil
    server = TCPServer.new(@config.clustering_bind, @config.clustering_raft_port)
    peers = @config.clustering_peer_addresses.reject(@config.clustering_raft_address)
    transport = @transport = Raft::TCPTransport.new(@config.clustering_secret, peers, ->@node.deliver(Raft::Message))
    spawn(transport.listen(server), name: "Raft listener")
    @node.run(transport)
  rescue ex : Socket::BindError
    abort "Error: #{ex.message}"
  end

  # Whether this node may win an election before it has any raft state. Only
  # safe when it can't hold data another node lacks (a new node, or the only
  # one), or when the operator says it has the latest data.
  private def may_bootstrap? : Bool
    return true if @config.clustering_bootstrap?
    return true if @config.clustering_peer_addresses.size == 1
    Dir.children(@config.data_dir).all? { |f| f.in?(".clustering_id", ".raft_state", ".lock") }
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

  # Replicate from the leader, switching whenever the leader changes, until
  # this node becomes the leader itself.
  private def follow_leader
    loop do
      if follow(current_leader_uri) == :elected
        Log.debug { "Elected leader, don't replicate from self" }
        return
      end
      select
      when @node.leader_changed.receive
      when @stop_signal.receive?
        return
      end
    end
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

  private def follow(uri : String?) : Symbol?
    if repli_client = @repli_client # is currently following a leader
      return if repli_client.follows? uri
      repli_client.close
      @repli_client = nil
    end
    if uri.nil?
      Log.warn { "No leader available" }
      return
    end
    if uri == @advertised_uri
      return :elected if leader?
      raise Error.new("Another node in the cluster is advertising the same URI")
    end
    Log.info { "Leader: #{uri}" }
    @repli_client = r = Clustering::Client.new(@config, @id, @coordinator.password)
    spawn r.follow(uri), name: "Clustering client #{uri}"
    SystemD.notify_ready
    nil
  end

  private def current_leader_uri : String?
    @node.leader_uri
  end

  private def leader? : Bool
    @node.leader?
  end

  # Votes are only granted to ISR members, so this can't trip unless the
  # raft state was tampered with. Serving without confirmed messages would
  # lose them cluster-wide, so refuse.
  private def ensure_in_isr! : Nil
    isr = @node.committed_isr
    return if isr.nil? || isr.includes?(@id)
    Log.fatal { "Elected leader but not in the in-sync replica set (ISR: #{isr.to_a}), stepping down" }
    exit 3
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
