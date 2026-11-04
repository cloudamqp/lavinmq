require "./controller"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"
require "../etcd"
require "../../stdlib/channel"

# Leader election and ISR storage by the nodes themselves, with Raft.
class LavinMQ::Clustering::RaftController < LavinMQ::Clustering::Controller
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  # Accepted leadership transfer: who (clustering id and raft address), and
  # in which term
  record Transfer, target : Int32, address : String, term : Int64

  @step_down_requested = Channel(Transfer).new(1)
  @transfer_target : Int32? = nil
  @transfer_lock = Mutex.new
  @stop_signal = Channel(Nil).new
  # Set once, by the first #stop
  @stopped = Atomic(Bool).new(false)
  # Closed by the follower monitor once this node is a serving leader, so
  # only that fiber decides between replicating and promoting.
  @promoted = Channel(Nil).new

  # Raft (the node's event loop and the transport's connections) runs in an
  # execution context of its own, a thread the broker's fibers don't run on.
  # Otherwise a busy default context could delay heartbeats past the election
  # timeout: followers would start elections and the leader would step down on
  # losing its quorum, while it's only busy. The work in it is small, so one
  # thread is enough. One for the process, it can't be stopped: a node runs
  # one controller, but specs run many.
  @@raft_context : Fiber::ExecutionContext::Concurrent? = nil
  @@raft_context_lock = Mutex.new

  private def raft_context : Fiber::ExecutionContext::Concurrent
    @@raft_context_lock.synchronize { @@raft_context ||= Fiber::ExecutionContext::Concurrent.new("Raft") }
  end

  def initialize(config : Config)
    FileSystem.mkdir_p config.data_dir
    storage = Raft::Storage.new(config.data_dir)
    state = storage.load
    has_data = Clustering.broker_data?(config.data_dir)
    # Its raft state has its id from the first start, but no log until the
    # cluster has a first leader
    no_log = state.nil? || (state.snapshot_index == 0 && state.entries.empty?)
    prior_in_sync = has_data && no_log && self.class.prior_in_sync?(config)
    unless state
      # Before anyone learns it, so it's never another id after a restart
      state = Raft::HardState.new(0, nil, 0, 0, nil, Array(Raft::Entry).new, nil, self.class.new_raft_id)
      storage.save(state)
    end
    super(config, state.node_id)
    @node = Raft::Node.new(@id, @config.clustering_raft_address, @config.clustering_seed_addresses,
      @advertised_uri, storage,
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      has_data: has_data, prior_in_sync: prior_in_sync, state: state, execution_context: raft_context)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  # The clustering id is kept in the raft state, see Raft::HardState. Without
  # a state the node is a new one, also when it was a member before: it can't
  # tell which terms it voted in, so it mustn't vote again with that id.
  protected def self.new_raft_id : Int32
    id = rand(Int32::MAX)
    Log.info do
      "No raft state, starting as a new node with clustering id #{id.to_s(36)}. If this node was a member " \
      "before, remove its old id from the cluster and add it again, see docs/clustering.md"
    end
    id
  end

  # Whether this node was in the ISR of the etcd backend, read when it starts
  # raft with broker data but no raft log, as after a migration, by the id it
  # had there, in .clustering_id. That tells the nodes whose data is current,
  # see Raft::Core#bootstrap_choice.
  protected def self.prior_in_sync?(config : Config) : Bool
    return false if config.clustering_seed_addresses.all?(config.clustering_raft_address)
    id = stored_clustering_id(config) || return false
    key = "#{config.clustering_etcd_prefix}/isr"
    raw = Etcd.new(config.clustering_etcd_endpoints).try_get(key) || return false
    in_sync = raw.split(',').any? { |i| i.to_i?(36) == id }
    Log.info { "The etcd backend's ISR (#{raw}) #{in_sync ? "includes" : "doesn't include"} this node" }
    in_sync
  rescue ex : ArgumentError
    Log.warn { "Can't read the etcd backend's ISR: #{ex.message}" }
    false
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
    return "This node is not the leader" if @stopping.get
    status = @node.status || return "The raft node didn't answer in time, try again"
    if target
      id = status.resolve(target) || return "#{target} is not a member"
    end
    case check = @node.transfer_check(id)
    in Tuple(Int32, Int64)
      to, term = check
      Transfer.new(to, status.address_of(to) || to.to_s(36), term)
    in Raft::TransferRefusal
      check.message(target)
    end
  end

  # Gracefully step down in favour of `target`: stop serving, hand over
  # leadership and continue as a follower, see #run. Returns right away, the
  # transfer was already claimed by #request_transfer.
  def step_down(target : Transfer) : Nil
    # Accepted in a term this node has lost since, it must neither be done
    # nor take the place of a later one
    return if stale?(target)
    unless @step_down_requested.try_send?(target)
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
      break if @stopping.get
      step_down_as_leader(transfer)
    end
    @stop_signal.receive?
  end

  def stop
    return if @stopped.swap(true)
    @stopping.set(true)
    @repli_client.get.try &.close
    # Before releasing #run, so the process can't exit while handing over
    hand_over_leadership
    @stop_signal.close
    @node.close
  end

  def cluster_status : String?
    status = @node.status || return
    # Replicating from it, which it lets this node do once it serves
    serving = @repli_client.get.try { |c| c.connected? && c.follows?(current_leader_uri) } || false
    JSON.build { |json| status.to_json(json, leader_serving: serving) }
  end

  def raft : RaftController?
    self
  end

  # The replication client's metrics while following, and the raft node's
  def write_metrics(writer) : Nil
    super
    @node.metrics.try &.write(writer)
  end

  # Follows the leader until this node is a serving leader. False if the
  # node is stopped first.
  private def await_promotion : Bool
    @promoted = Channel(Nil).new
    spawn(name: "Follower monitor") { follow_leader { follow_leader_changes } }
    select
    when @promoted.receive?
    when @stop_signal.receive?
      return false
    end
    !@stopping.get
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
  rescue Coordinator::StaleLeadership
    # Lost while starting to serve, replicated writes during startup fail
    # with it, also when a fenced replicator or the Persister raises it
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
    @step_down_requested.try_receive?
    @transfer_lock.synchronize { @transfer_target = nil }
  end

  # Lets an in-sync follower take over right away instead of after an
  # election timeout.
  private def hand_over_leadership : Nil
    return unless leader?
    target = @transfer_lock.synchronize { @transfer_target }
    result = @node.transfer_leadership(target)
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
    transport = Raft::TCPTransport.new(@config.clustering_secret, @id, address, peers,
      ->@node.deliver(Raft::TransportEvent), execution_context: raft_context)
    raft_context.spawn(name: "Raft listener") { transport.listen(server) }
    @node.run(transport)
  rescue ex : Socket::BindError
    abort "Error: #{ex.message}"
  end

  # Switch leader to replicate from whenever the leader changes, until this
  # node is a serving leader. A leadership lost before it could serve goes
  # back to following whoever leads next.
  private def follow_leader_changes
    loop do
      if follow(leadership) == :elected
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

  private def follow(leadership : Raft::Node::Leadership) : Symbol?
    uri = leadership.uri
    if repli_client = @repli_client.get # is currently following a leader
      return if repli_client.follows? uri
      stop_following
    end
    if uri.nil?
      Log.warn { "No leader available" }
      return
    end
    if uri == @advertised_uri
      return :elected if leadership.leader
      raise Error.new("Another node in the cluster is advertising the same URI")
    end
    Log.info { "Leader: #{uri}" }
    r = Clustering::Client.new(@config, @id, @coordinator.password)
    r.member_check = -> { @node.self_member? }
    r.on_host_change = ->leader_host_changed
    @repli_client.set(r)
    spawn r.follow(uri), name: "Clustering client #{uri}"
    SystemD.notify_ready
    nil
  end

  private def stop_following : Nil
    @repli_client.swap(nil).try &.close
    leader_host_changed
  end

  private def current_leader_uri : String?
    @node.leader_uri
  end

  private def leadership : Raft::Node::Leadership
    @node.leadership
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
