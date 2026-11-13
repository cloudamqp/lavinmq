require "../http/metrics_server"
require "../etcd"
require "../filesystem"
require "./client"
require "./etcd_coordinator"
require "wait_group"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32

  @repli_client : Client? = nil
  @lease : Etcd::Lease? = nil

  def self.new(config : Config)
    etcd = Etcd.new(config.clustering_etcd_endpoints)
    new(config, etcd, EtcdCoordinator.new(config, etcd))
  end

  # The metrics server, if any, is pointed at each follower client, so it keeps
  # serving on the same socket across leader changes
  def initialize(@config : Config, @etcd : Etcd, @coordinator : EtcdCoordinator,
                 @metrics_server : HTTP::MetricsServer? = nil)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
    @elected_leader = BoolChannel.new(false)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.

  def run(&)
    lease = @lease = @etcd.lease_grant(ttl: @config.clustering_etcd_lease_ttl, id: @id)
    lease_released = WaitGroup.new
    lease_released.spawn(name: "Lease watcher") { watch_lease(lease) }
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync
    @coordinator.campaign(@advertised_uri, @id) # blocks until becoming leader, captures the fencing token
    @elected_leader.set(true)
    ensure_in_isr!
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close
    yield
    lease_released.wait # until #stop releases the lease
  rescue Etcd::LeaseAlreadyExists
    Log.fatal { "Cluster ID #{@id.to_s(36)} used by another node" }
    exit 3
  rescue Etcd::LeaseNotFound
    Log.fatal { "Lease not found, etcd may have been reset" }
    exit 3
  end

  # Exits when the lease expires, whether this node follows, campaigns, starts
  # or serves: etcd answers a campaign on an expired lease as won
  private def watch_lease(lease : Etcd::Lease) : Nil
    loop do
      lease.wait(1.hour) # blocks until the lease expires (raises Expired)
    end
  rescue ex : Etcd::Lease::Expired
    stopping = @stopping.get
    return if stopping.stepping_down? # never led, so no leader_lost hook
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost") if @elected_leader.value
    if stopping.none?
      Log.fatal { "Lease expired: #{ex.cause.try(&.message) || ex.message}" }
      exit 3
    end
  end

  # Why this node releases its lease, set before it does so the lease watcher
  # can tell a release from a lost lease
  enum Stopping
    None
    Stop         # #stop
    SteppingDown # #ensure_in_isr!
  end

  @stopping = Atomic(Stopping).new(Stopping::None)

  def stop
    @stopping.set(Stopping::Stop)
    @repli_client.try &.close
    @lease.try &.release
  end

  # Each node in a cluster has an unique id, for tracking ISR
  private def clustering_id : Int32
    id_file_path = File.join(@config.data_dir, ".clustering_id")
    begin
      id = File.read(id_file_path).to_i(36)
    rescue File::NotFoundError
      id = rand(Int32::MAX)
      FileSystem.mkdir_p @config.data_dir
      FileSystem.replace(id_file_path, &.print(id.to_s(36)))
      Log.info { "Generated new clustering ID" }
    end
    id
  end

  # Replicate from the leader
  # Listens for leader change events
  private def follow_leader
    @etcd.elect_listen("#{@config.clustering_etcd_prefix}/leader") do |uri|
      if repli_client = @repli_client # is currently following a leader
        if repli_client.follows? uri
          next # if lost connection to etcd we continue follow the leader as is
        else
          # Waits for a close already in progress, so the ports are free before
          # the next client binds them, but it also holds up later leader events
          repli_client.close
        end
      end
      if uri.nil? # no leader yet
        Log.warn { "No leader available" }
        next
      end
      if uri == @advertised_uri # if this instance has become leader
        select
        when @elected_leader.when_true.receive
          Log.debug { "Elected leader, don't replicate from self" }
          @elected_leader.close
          return
        when timeout(1.second)
          raise Error.new("Another node in the cluster is advertising the same URI")
        end
      end
      Log.info { "Leader: #{uri}" }
      key = "#{@config.clustering_etcd_prefix}/clustering_secret"
      secret = @etcd.get(key)
      until secret # the leader might not have had time to set the secret yet
        Log.debug { "Clustering secret is missing, watching for it" }
        @etcd.watch(key) do |value|
          secret = value
          break
        end
      end
      @repli_client = r = Clustering::Client.new(@config, @id, secret)
      @metrics_server.try &.follower = r
      spawn r.follow(uri), name: "Clustering client #{uri}"
      SystemD.notify_ready
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

  # A queued election candidacy can outlive ISR membership: this node may have
  # been dropped from the ISR (lagging or disconnected replication) after it
  # campaigned, and etcd's election doesn't consult the ISR key. Serving as
  # leader while missing confirmed messages would lose them cluster-wide — the
  # in-sync nodes would full_sync from us and delete them. Exit instead: that
  # releases the lease, withdraws the candidacy and lets an in-sync candidate
  # win; on restart wait_to_be_insync blocks until this node is re-synced.
  private def ensure_in_isr! : Nil
    isr = @coordinator.isr
    return if isr.nil? # no ISR recorded yet (fresh cluster)
    return if isr.includes?(@id)
    Log.fatal { "Won the leader election but is not in the in-sync replica set (ISR: #{isr.to_a}), stepping down" }
    # Release the lease explicitly: the election key is bound to it, so this
    # revokes the just-won leadership at once instead of leaving the cluster
    # leaderless until the lease TTL expires.
    @stopping.set(Stopping::SteppingDown)
    begin
      @lease.try &.release
    rescue ex
      Log.warn(exception: ex) { "Failed to release lease while stepping down, it will expire on its own" }
    end
    exit 3
  end

  def wait_to_be_insync
    if isr = @coordinator.isr
      unless isr.includes?(@id)
        Log.info { "ISR: #{isr.to_a}" }
        Log.info { "Not in sync, waiting for a leader" }
        in_sync = Channel(Nil).new
        spawn do
          @coordinator.watch_isr do |members|
            if members.try &.includes?(@id)
              in_sync.close
              break
            end
          end
        end
        in_sync.receive?
        Log.info { "In sync with leader" }
      end
    end
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
