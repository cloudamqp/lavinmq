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
  # The lavinmqctl socket, bound once for the process: the broker's API
  # while serving (see #control_api=), this node's own view otherwise
  @control_socket : HTTP::ControlSocket? = nil
  # Stops serving as the leader, see #on_demote
  @demote : (Proc(Nil)? ->)? = nil
  @step_down_requested = Channel(Transfer).new(1)
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

  # How long stopping to serve, or a startup that lost leadership, may take
  # before the process exits instead, so a node that isn't the leader anymore
  # never keeps serving
  property demotion_timeout : Time::Span = 60.seconds

  # Registers how the Launcher stops serving when this node stops being the
  # leader, without exiting: the raft node keeps running and this node
  # follows the new leader. When leadership is handed over the block gets
  # the handover to call once clients are disconnected, while the followers
  # are still connected (the target has to stay in the ISR). When it was
  # lost, it gets nil.
  def on_demote(&block : Proc(Nil)? ->) : Nil
    @demote = block
  end

  # Checks that `target` (a clustering id or raft address, or without one any
  # in-sync voter, preferably caught up) can take over right now, see
  # Raft::Core#transfer_check, or the leader would stop serving for a
  # handover that can't happen. Returns the accepted transfer, or why not. An
  # accepted transfer is claimed here, so a concurrent request is refused
  # instead of overriding it, and #step_down has to follow.
  def request_transfer(target : String? = nil) : Transfer | String
    @transfer_lock.synchronize do
      return "A leadership transfer is already in progress" if @transfer_target
      plan = check_transfer(target)
      @transfer_target = plan.target if plan.is_a?(Transfer)
      plan
    end
  end

  private def check_transfer(target : String?) : Transfer | String
    status = @node.status
    return "This node is not the leader" if status.nil? || @stopping
    if target
      id = status.resolve(target) || return "#{target} is not a member"
    end
    case check = @node.transfer_check(id)
    in Tuple(Int32, Int64)
      to, term = check
      Transfer.new(to, status.address_of(to) || to.to_s(36), term)
    in Raft::TransferRefusal
      refusal_message(check, target)
    end
  end

  private def refusal_message(refusal : Raft::TransferRefusal, target : String?) : String
    case refusal
    in .not_leader?        then "This node is not the leader"
    in .not_serving?       then "The leader hasn't committed an entry in its term yet"
    in .is_leader?         then "#{target} is the leader"
    in .not_member?        then "#{target} is not a member"
    in .not_voter?         then "#{target} is not a voter"
    in .not_in_isr?        then "#{target} is not in the in-sync replica set"
    in .unresponsive?      then "#{target} hasn't answered the leader recently"
    in .no_eligible_voter? then "No reachable voter is in the in-sync replica set"
    end
  end

  # Gracefully step down in favour of `target`: stop serving, hand over
  # leadership and continue as a follower, see #run. Returns right away, the
  # transfer was already claimed by #request_transfer.
  def step_down(target : Transfer) : Nil
    # Accepted in a term this node has lost since, it must neither be done
    # nor take the place of a later one
    return if stale?(target)
    select
    when @step_down_requested.send(target)
    else
      Log.warn { "A step down is already pending, not handing over to #{target.address}" }
      # Don't leave the claim behind, or no later transfer could be requested
      @transfer_lock.synchronize { @transfer_target = nil if @transfer_target == target.target }
    end
  end

  # Follows the leader, and yields to start serving whenever this node
  # becomes the leader. When it stops being the leader (leadership lost, or
  # handed over) the broker is stopped (see #on_demote) and this node
  # follows again, all in this process: the raft node keeps running, so a
  # node that hands over leadership still counts for the new leader's quorum.
  # Returns once the node is stopped.
  def run(&)
    start_node
    loop do
      break unless await_promotion
      transfer = lead { yield }
      break if @stopping
      step_down_as_leader(transfer)
    end
    @stop_signal.receive?
  end

  def stop
    return if @stopped
    @stopped = @stopping = true
    @repli_client.try &.close
    @control_socket.try &.close
    # Before releasing #run, so the process can't exit while handing over
    hand_over_leadership
    @stop_signal.close
    @node.close
  end

  private def local_status : String?
    status = @node.status || return
    JSON.build { |json| status.to_json(json) }
  end

  # Where lavinmqctl requests go while this node serves, nil once it stops.
  # The socket is bound if it couldn't be when the node started, e.g. while
  # another node on this machine had it.
  def control_api=(handler : ::HTTP::Handler?) : Nil
    socket = @control_socket || return
    socket.bind if handler
    socket.api = handler
  end

  # The path the lavinmqctl socket was bound at, which a config reload can't change
  def control_path : String
    @control_socket.try(&.path) || @config.control_unix_path
  end

  # Follows the leader until this node is a serving leader. False if the
  # node is stopped first.
  private def await_promotion : Bool
    @promoted = Channel(Nil).new
    spawn(follow_leader, name: "Follower monitor")
    select
    when @promoted.receive?
    when @stop_signal.receive?
      return false
    end
    !@stopping
  end

  # Serves as the leader until leadership is lost, a transfer is requested,
  # or the node stops. Returns the requested transfer, if that's why.
  private def lead(&) : Transfer?
    ensure_in_isr!
    stop_following
    # No follower is replicating from this node yet, so none of them can be
    # trusted to have what it's about to confirm. They rejoin the ISR as they
    # finish syncing.
    @coordinator.update_isr(Set{@id})
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    # Replicated writes fail once leadership is lost, which ends a startup
    # that waits for them, but exit if it hangs on something else
    started = Channel(Nil).new
    spawn(watchdog(started, "Lost leadership while starting to serve, and the startup didn't stop, exiting",
      armed: @node.serving.when_false), name: "Startup watchdog")
    begin
      yield
    ensure
      started.close
    end
    loop do
      select
      when @node.serving.when_false.receive
        return
      when transfer = @step_down_requested.receive
        return transfer unless stale?(transfer)
      when @stop_signal.receive?
        return
      end
    end
  rescue RaftCoordinator::StaleLeadership
    # Lost while starting to serve, replicated writes during startup fail
    # with it
    nil
  end

  # A transfer whose request raced with losing leadership can reach a later
  # term, where nobody asked for it
  private def stale?(transfer : Transfer) : Bool
    term = @node.term
    return false if term == transfer.term
    Log.warn { "Ignoring a leadership transfer to #{transfer.address} requested in term #{transfer.term}, now #{term}" }
    true
  end

  private def step_down_as_leader(transfer : Transfer?) : Nil
    if transfer
      Log.warn { "Stepping down, handing over leadership to #{transfer.address}" }
    else
      Log.warn { "Lost leadership, continuing as a follower" }
    end
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    if transfer
      demote(->hand_over_leadership)
      Log.warn { "Leadership wasn't handed over to #{transfer.address}, serving again" } if leader?
    else
      demote(nil)
    end
    # A transfer requested as leadership was lost is void, it mustn't be done
    # in a later term or block the next one
    select
    when @step_down_requested.receive?
    else
    end
    @transfer_lock.synchronize { @transfer_target = nil }
  end

  # Exits unless *done* is closed within the demotion timeout, counted from
  # when *armed* fires, or right away without it: a node that isn't the
  # leader anymore must never keep serving.
  private def watchdog(done : Channel(Nil), message : String, armed : Channel(Nil)? = nil) : Nil
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
      if demote = @demote
        demote.call(hand_over)
      else
        hand_over.try &.call
      end
    rescue ex
      Log.fatal(exception: ex) { "Failed to stop serving, exiting" }
      exit 3
    ensure
      done.close
    end
  end

  # Lets an in-sync follower take over right away instead of after an
  # election timeout.
  private def hand_over_leadership : Nil
    return unless leader?
    result = @node.transfer_leadership(@transfer_target)
    unless result.sent? || result.pending?
      Log.warn { "Leadership transfer refused: #{result}" }
      return
    end
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
    @control_socket = HTTP::ControlSocket.new(@config.control_unix_path, ->local_status).tap(&.bind)
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
      stop_following
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
    report_metrics_of r
    r.member_check = -> { @node.self_member? }
    r.serve_control_socket = false
    spawn r.follow(uri), name: "Clustering client #{uri}"
    SystemD.notify_ready
    nil
  end

  private def stop_following : Nil
    @repli_client.try &.close
    @repli_client = nil
    report_metrics_of nil
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
