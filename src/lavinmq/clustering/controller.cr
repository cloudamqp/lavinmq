require "systemd"
require "./client"
require "./raft_coordinator"
require "./raft/node"
require "./raft/transport"
require "./etcd_seed"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32
  getter coordinator : RaftCoordinator
  getter node : Raft::Node

  @repli_client : Client? = nil
  @repli_password : String? = nil
  @transport : Raft::TCPTransport? = nil
  @etcd_seed : EtcdSeed? = nil
  @etcd_leader_uri : String? = nil
  @etcd_secret : String? = nil
  @etcd_changed = Channel(Nil).new(1)

  def initialize(@config : Config)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
    storage = Raft::Storage.new(@config.data_dir)
    if migrating_from_etcd?(storage)
      @etcd_seed = EtcdSeed.new(@config.clustering_etcd_endpoints, @config.clustering_etcd_prefix)
    end
    @node = Raft::Node.new(@config.clustering_raft_address, @config.clustering_peer_addresses,
      @id, @advertised_uri, storage,
      @config.clustering_election_timeout.milliseconds, @config.clustering_heartbeat_interval.milliseconds,
      bootstrap: may_bootstrap?, campaign: @etcd_seed.nil?)
    @coordinator = RaftCoordinator.new(@node, @config.clustering_secret)
  end

  # A node without raft state in a cluster that ran on etcd: until the etcd
  # leader is gone it can't know whether its data is current. `bootstrap`
  # overrides that, like it does without etcd.
  private def migrating_from_etcd?(storage : Raft::Storage) : Bool
    return false if @config.clustering_etcd_endpoints.empty?
    return false if @config.clustering_bootstrap?
    !File.exists?(storage.path)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  def run(&)
    start_node
    if seed = @etcd_seed
      spawn(migrate_from_etcd(seed), name: "etcd migration")
    end
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

  # Rolling migration from an etcd-coordinated cluster. While the etcd leader
  # holds its lease, follow it like an etcd-era follower would and don't
  # campaign. Once the lease is gone the etcd ISR can no longer change (only
  # the lease holder writes it), so every node seeds raft with the same,
  # final ISR, and only a node in it can win: the cluster picks up where etcd
  # left off. A node that hears from a raft leader first has its state from
  # it instead.
  private def migrate_from_etcd(seed : EtcdSeed) : Nil
    Log.info { "No raft state, migrating from etcd at #{@config.clustering_etcd_endpoints}" }
    until @stopped
      return joined_raft if @node.leader_uri
      begin
        election = seed.election
        unless holder = election.leader_uri
          isr = seed.isr
          @node.seed(isr)
          Log.info { "etcd leader gone, seeded raft with the etcd ISR #{isr.try(&.to_a) || "(none)"}" }
          set_etcd_leader nil
          return
        end
        @etcd_secret ||= seed.clustering_secret
        # Our own previous incarnation's lease: nothing to follow, wait for it
        # to expire
        set_etcd_leader(holder == @advertised_uri ? nil : holder)
        return unless wait_for_election_change(seed, election.revision)
      rescue ex : EtcdSeed::Error | IO::Error | Socket::Error
        return if @stopped
        Log.warn { "Can't read etcd, retrying: #{ex.message}" }
        select
        when @node.leader_changed.receive
        when @stop_signal.receive?
          return
        when timeout(1.second)
        end
      end
    end
  ensure
    seed.close
  end

  # Waits on an etcd watch, so a takeover or the lease going away is acted on
  # at once. Returns false when there's nothing more to do here: stopping, or
  # a raft leader turned up (this node has its state from it then).
  private def wait_for_election_change(seed : EtcdSeed, revision : Int64) : Bool
    changed = Channel(Exception?).new(1)
    spawn(name: "etcd election watch") do
      seed.wait_for_election_change(revision)
      changed.send nil
    rescue ex
      changed.send ex
    end
    loop do
      select
      when ex = changed.receive
        raise ex if ex
        return true
      when @node.leader_changed.receive
        if @node.leader_uri
          joined_raft
          return false
        end
      when @stop_signal.receive?
        return false
      end
    end
  end

  private def joined_raft : Nil
    @node.seed(nil) # has state from the raft leader, nothing is seeded
    Log.info { "Joined the raft cluster, etcd is no longer used" }
    set_etcd_leader nil
  end

  private def set_etcd_leader(uri : String?) : Nil
    return if uri == @etcd_leader_uri
    @etcd_leader_uri = uri
    select
    when @etcd_changed.send(nil)
    else
    end
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
      when @etcd_changed.receive
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
    # The etcd-era leader authenticates followers with the secret it keeps in
    # etcd, a raft leader with the configured password.
    password = if uri && uri == @etcd_leader_uri && @node.leader_uri.nil?
                 @etcd_secret || @coordinator.password
               else
                 @coordinator.password
               end
    if repli_client = @repli_client # is currently following a leader
      return if repli_client.follows?(uri) && password == @repli_password
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
    @repli_password = password
    @repli_client = r = Clustering::Client.new(@config, @id, password)
    spawn r.follow(uri), name: "Clustering client #{uri}"
    SystemD.notify_ready
    nil
  end

  private def current_leader_uri : String?
    @node.leader_uri || @etcd_leader_uri
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
