require "./controller"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"

# Leader election and ISR storage by the nodes themselves, with Raft.
class LavinMQ::Clustering::RaftController < LavinMQ::Clustering::Controller
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  # Accepted leadership transfer: who (clustering id and raft address), and
  # in which term
  record Transfer, target : Int32, address : String, term : Int64

  @transport : Raft::TCPTransport? = nil
  # Serves lavinmqctl this node's view of the cluster until it leads
  @control_server : ::HTTP::Server? = nil
  @step_down : (String ->)? = nil
  @transfer_target : Int32? = nil
  @transfer_lock = Mutex.new
  @stop_signal = Channel(Nil).new
  # Closed by the follower monitor once this node is a serving leader, so
  # only that fiber decides between replicating and promoting.
  @promoted = Channel(Nil).new

  # Raft (the node's event loop and the transport's connections) runs in an
  # execution context of its own, a thread the broker's fibers don't run on.
  # Otherwise a busy default context could delay heartbeats past the election
  # timeout: followers would start elections and the leader would step down on
  # losing its quorum, while it's only busy. The work in it is small, so one
  # thread is enough.
  @raft_context = Fiber::ExecutionContext::Concurrent.new("Raft")

  def initialize(config : Config)
    super(config)
    @node = Raft::Node.new(@id, @config.clustering_raft_address, @config.clustering_seed_addresses,
      @advertised_uri, Raft::Storage.new(@config.data_dir),
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      bootstrap: may_bootstrap?, execution_context: @raft_context)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  # Registers what to do when an operator asks this leader to hand over
  # leadership, see #step_down. The Launcher shuts the node down gracefully.
  def on_step_down(&block : String ->) : Nil
    @step_down = block
  end

  # Checks that `target` (a clustering id or raft address, or without one any
  # caught up in-sync voter) can take over right now: it must be a voter in
  # the committed ISR that has answered the leader within the election
  # timeout, or the leader would stop serving for a handover that can't
  # happen. Returns the accepted transfer, or why not. An accepted
  # transfer is claimed here, so a concurrent request is refused instead of
  # overriding it, and #step_down has to follow.
  def request_transfer(target : String? = nil) : Transfer | String
    @transfer_lock.synchronize do
      return "A leadership transfer is already in progress" if @transfer_target
      plan = check_transfer(target)
      @transfer_target = plan.target if plan.is_a?(Transfer)
      plan
    end
  end

  # ameba:disable Metrics/CyclomaticComplexity
  private def check_transfer(target : String?) : Transfer | String
    status = @node.status
    return "This node is not the leader" if status.nil? || !status.role.leader? || @stopping
    return "The leader hasn't committed an entry in its term yet" unless @node.serving.value
    voters = status.membership.try(&.voters) || return "The cluster has no membership yet"
    isr = status.committed_isr || return "The cluster has no in-sync replica set yet"
    if target
      id = status.resolve(target) || return "#{target} is not a member"
      return "#{target} is the leader" if id == status.id
      return "#{target} is not a voter" unless voters.includes?(id)
      return "#{target} is not in the in-sync replica set" unless isr.includes?(id)
      return "#{target} hasn't answered the leader recently" unless status.responsive.includes?(id)
    else
      eligible = voters.select { |v| v != status.id && isr.includes?(v) && status.responsive.includes?(v) }
      id = eligible.find { |v| status.caught_up.includes?(v) } || eligible.first? ||
           return "No reachable voter is in the in-sync replica set"
    end
    Transfer.new(id, status.address_of(id) || id.to_s(36), status.term)
  end

  # Gracefully step down in favour of `target`: stop serving, hand over
  # leadership and restart as a follower, see Launcher#step_down.
  def step_down(target : Transfer) : Nil
    if callback = @step_down
      callback.call(target.address)
    else
      Log.warn { "No step down handler registered, can't hand over leadership to #{target.address}" }
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
    # The leader's HTTP server binds the control socket when it starts
    close_control_server
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
    close_control_server
    # Before releasing #run, so the process can't exit while handing over
    hand_over_leadership
    @stop_signal.close
    @node.close
  end

  private def local_status : String?
    status = @node.status || return
    JSON.build { |json| status.to_json(json) }
  end

  private def close_control_server : Nil
    @control_server.try &.close
    @control_server = nil
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
    address = @config.clustering_raft_address
    peers = @config.clustering_seed_addresses.reject(address)
    transport = @transport = Raft::TCPTransport.new(@config.clustering_secret, @id, address, peers,
      ->@node.deliver(Raft::TransportEvent), execution_context: @raft_context)
    @raft_context.spawn(name: "Raft listener") { transport.listen(server) }
    @node.run(transport)
    @control_server = HTTP::Server.follower_internal_socket_http_server(->local_status)
  rescue ex : Socket::BindError
    abort "Error: #{ex.message}"
  end

  # Whether this node may win an election before it has any raft state: when
  # it's the only seed, or when the operator says it has the latest data. An
  # empty data dir isn't enough, a majority of new nodes would then elect one
  # of themselves and wipe the data of the nodes that have it.
  private def may_bootstrap? : Bool
    @config.clustering_bootstrap? || @config.clustering_seed_addresses.all?(@config.clustering_raft_address)
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
    r.serve_control_socket = false
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
