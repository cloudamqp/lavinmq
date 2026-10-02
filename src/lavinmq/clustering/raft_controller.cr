require "./controller"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"

# Leader election and ISR storage by the nodes themselves, with Raft.
class LavinMQ::Clustering::RaftController < LavinMQ::Clustering::Controller
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  # Accepted leadership transfer: who, and in which term
  record Transfer, target : String, term : Int64

  @transport : Raft::TCPTransport? = nil
  @step_down : (String ->)? = nil
  @transfer_target : String? = nil
  @stop_signal = Channel(Nil).new
  # Closed by the follower monitor once this node is a serving leader, so
  # only that fiber decides between replicating and promoting.
  @promoted = Channel(Nil).new

  def initialize(config : Config)
    super(config)
    @node = Raft::Node.new(@config.clustering_raft_address, @config.clustering_peer_addresses,
      @id, @advertised_uri, Raft::Storage.new(@config.data_dir),
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      bootstrap: may_bootstrap?)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  # Registers what to do when an operator asks this leader to hand over
  # leadership, see #step_down. The Launcher shuts the node down gracefully.
  def on_step_down(&block : String ->) : Nil
    @step_down = block
  end

  # Checks that `target` (a raft address, or without one any caught up
  # in-sync voter) can take over right now: it must be a voter in the
  # committed ISR with a known clustering id. Returns the accepted transfer,
  # or why not. Nothing changes until #step_down.
  # ameba:disable Metrics/CyclomaticComplexity
  def request_transfer(target : String? = nil) : Transfer | String
    status = @node.status
    return "This node is not the leader" if status.nil? || !status.role.leader? || @stopping
    return "The leader hasn't committed an entry in its term yet" unless @node.serving.value
    voters = status.membership.try(&.voters) || return "The cluster has no membership yet"
    isr = status.committed_isr || return "The cluster has no in-sync replica set yet"
    if target
      return "#{target} is the leader" if target == status.address
      return "#{target} is not a voter" unless voters.includes?(target)
      return "#{target} is not in the in-sync replica set" unless transfer_eligible?(status, voters, isr, target)
    else
      target = voters.find { |a| transfer_eligible?(status, voters, isr, a) && status.caught_up.includes?(a) } ||
               voters.find { |a| transfer_eligible?(status, voters, isr, a) } ||
               return "No voter is in the in-sync replica set"
    end
    Transfer.new(target, status.term)
  end

  private def transfer_eligible?(status : Raft::Status, voters : Set(String), isr : Set(Int32), addr : String) : Bool
    return false if addr == status.address || !voters.includes?(addr)
    id = status.node_id_of(addr)
    !id.nil? && isr.includes?(id)
  end

  # Gracefully step down in favour of `target`: stop serving, hand over
  # leadership and restart as a follower, see Launcher#step_down.
  def step_down(target : String) : Nil
    @transfer_target = target
    if callback = @step_down
      callback.call(target)
    else
      Log.warn { "No step down handler registered, can't hand over leadership to #{target}" }
    end
  end

  def run(&)
    start_node
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
    @stop_signal.receive?
  rescue RaftCoordinator::StaleLeadership
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopping
      Log.fatal { "Lost leadership before starting to serve" }
      exit 3
    end
  end

  def stop
    return if @stopped
    @stopped = @stopping = true
    @repli_client.try &.close
    # Before releasing #run, so the process can't exit while handing over
    hand_over_leadership
    @stop_signal.close
    @node.close
  end

  private def exit_on_leadership_loss : Nil
    @node.serving.when_false.receive
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    return if @stopping
    Log.fatal { "Lost leadership" }
    exit 3
  end

  # Lets an in-sync follower take over right away instead of after an
  # election timeout.
  private def hand_over_leadership : Nil
    return unless leader?
    result = @node.transfer_leadership(@transfer_target)
    return unless result.sent? || result.pending?
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
    r.member_check = -> { @node.self_member? }
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
