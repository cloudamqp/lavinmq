require "./controller"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"
require "./status_server"

# Leader election and ISR storage by the nodes themselves, with Raft.
class LavinMQ::Clustering::RaftController < LavinMQ::Clustering::Controller
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  @transport : Raft::TCPTransport? = nil
  @stop_signal = Channel(Nil).new
  # Closed by the follower monitor once this node is a serving leader, so
  # only that fiber decides between replicating and promoting.
  @promoted = Channel(Nil).new
  @status_server : StatusServer? = nil
  @started = false
  @ready_lock = Mutex.new

  def initialize(config : Config)
    super(config)
    @node = Raft::Node.new(@config.clustering_raft_address, @config.clustering_peer_addresses,
      @id, @advertised_uri, Raft::Storage.new(@config.data_dir),
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      bootstrap: may_bootstrap?)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  def run(&)
    start_node
    start_status_server
    spawn(follow_leader, name: "Follower monitor")
    select
    when @promoted.receive?
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
    # Startup can block on replicated writes that never complete without
    # leadership, so watch for its loss from here on, not after the yield.
    spawn(exit_on_leadership_loss, name: "Leadership monitor")
    yield
    @started = true
    update_ready
    @stop_signal.receive?
  rescue RaftCoordinator::StaleLeadership
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopping
      Log.fatal { "Lost leadership before starting to serve" }
      exit 3
    end
  end

  def stopping : Nil
    super
    update_ready
  end

  def stop
    return if @stopped
    @stopped = @stopping = true
    update_ready
    @status_server.try &.close
    @stop_signal.close
    @repli_client.try &.close
    hand_over_leadership
    @node.close
  end

  private def exit_on_leadership_loss : Nil
    @node.serving.when_false.receive
    update_ready
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    return if @stopping
    Log.fatal { "Lost leadership" }
    exit 3
  end

  # Ready means route clients here: serving, started up and not shutting
  # down. Recomputed under a lock so a stale caller can't overwrite a newer
  # state.
  private def update_ready : Nil
    @ready_lock.synchronize do
      @node.status.ready = @started && !@stopping && @node.serving.value
    end
  end

  # Optional, so a failure to bind is logged rather than stopping the node.
  private def start_status_server : Nil
    path = @config.clustering_status_unix_path
    return if path.empty?
    status_server = StatusServer.new(@node.status, path)
    begin
      status_server.bind
    rescue ex
      Log.warn { "Not serving the clustering status socket: #{ex.message}" }
      return
    end
    @status_server = status_server
    spawn(status_server.listen, name: "Clustering status listener")
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

  # Whether this node may win an election before it has any raft state: when
  # it's the only node, or when the operator says it has the latest data. An
  # empty data dir isn't enough, a majority of new nodes would then elect one
  # of themselves and wipe the data of the nodes that have it.
  private def may_bootstrap? : Bool
    @config.clustering_bootstrap? || @config.clustering_peer_addresses.size == 1
  end

  # Switch leader to replicate from whenever the leader changes, until this
  # node is a serving leader. A leadership lost before it could serve goes
  # back to following whoever leads next.
  private def follow_leader_changes
    loop do
      if follow(current_leader_uri) == :elected
        select
        when serving.when_true.receive
          Log.debug { "Elected leader, don't replicate from self" }
          @promoted.close
          return
        when @node.leader_changed.receive
        when @stop_signal.receive?
          return
        end
      else
        select
        when @node.leader_changed.receive
        when @stop_signal.receive?
          return
        end
      end
    end
  end

  private def serving : BoolChannel
    @node.serving
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
end
