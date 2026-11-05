require "../etcd"
require "./controller"
require "./etcd_coordinator"

# Leader election and ISR storage in an external etcd cluster.
class LavinMQ::Clustering::EtcdController < LavinMQ::Clustering::Controller
  getter coordinator : EtcdCoordinator

  @lease : Etcd::Lease? = nil
  # Makes ending a term's following and the follower monitor setting a
  # replication client one step each, see #stop_following
  @follow_lock = Mutex.new

  def self.new(config : Config)
    etcd = Etcd.new(config.clustering_etcd_endpoints)
    new(config, etcd, EtcdCoordinator.new(config, etcd))
  end

  def initialize(config : Config, @etcd : Etcd, @coordinator : EtcdCoordinator)
    super(config, Controller.clustering_id(config))
  end

  # Follows the leader, and yields to start serving whenever this node is
  # elected. When its lease expires the broker is stopped (see #on_demote),
  # and this node grants itself a new lease and follows again, all in this
  # process. Returns once stopped.
  def run(&)
    loop do
      lease = @lease = @etcd.lease_grant(ttl: @config.clustering_etcd_lease_ttl, id: @id)
      if @stopping.get # #stop released the previous lease, if any
        lease.release rescue nil
        return
      end
      # This term's, a follower monitor of an earlier term stops at its next
      # event, see #follow_leader_changes
      elected = BoolChannel.new(false)
      spawn(name: "Follower monitor") { follow_leader { follow_leader_changes(elected) } }
      wait_to_be_insync(lease)
      return if @stopping.get
      @coordinator.campaign(@advertised_uri, @id) # blocks until becoming leader, captures the fencing token
      stop_following(elected)
      ensure_in_isr!
      execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
      lead(lease) { yield }
      execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
      return if @stopping.get
      Log.warn { "Lost leadership, continuing as a follower" }
      # Stops its keepalive, in case leadership was lost some other way
      lease.release rescue nil
      demote(nil)
    end
  rescue Etcd::LeaseAlreadyExists
    Log.fatal { "Cluster ID #{@id.to_s(36)} used by another node" }
    exit 3
  rescue Etcd::LeaseNotFound
    Log.fatal { "Lease not found, etcd may have been reset" }
    exit 3
  end

  # Serves until the lease expires, or #stop releases it. Replicated writes
  # fail once leadership is lost, which ends a startup that waits for them,
  # but exits if it hangs on something else.
  private def lead(lease : Etcd::Lease, &) : Nil
    started = Channel(Nil).new
    spawn(watchdog(started, "Lost leadership while starting to serve, and the startup didn't stop, exiting",
      armed: lease.expired), name: "Startup watchdog")
    begin
      yield
    ensure
      started.close
    end
    loop do
      lease.wait(1.hour) # blocks until the lease expires (raises Expired)
    end
  rescue Etcd::Lease::Expired
  rescue Coordinator::StaleLeadership
    # Lost while starting to serve, replicated writes during startup fail
    # with it, also when a fenced replicator or the Persister raises it
  end

  def stop
    @stopping.set(true)
    @repli_client.get.try &.close
    @lease.try &.release
  end

  # This node was elected in the term of *elected*: its follower monitor
  # sets no replication client from here on, and it stops at its next event
  private def stop_following(elected : BoolChannel) : Nil
    client = @follow_lock.synchronize do
      elected.set(true)
      @repli_client.swap(nil)
    end
    client.try &.close
    leader_host_changed # connections wait for this node to serve
  end

  # Follows whoever leads until this node is elected in the term of
  # *elected*, see #stop_following
  private def follow_leader_changes(elected : BoolChannel)
    @etcd.elect_listen("#{@config.clustering_etcd_prefix}/leader") do |uri|
      return if elected.value || @stopping.get
      if repli_client = @repli_client.get # is currently following a leader
        if repli_client.follows? uri
          next # if lost connection to etcd we continue follow the leader as is
        else
          repli_client.close
          @repli_client.compare_and_set(repli_client, nil)
          leader_host_changed
        end
      end
      if uri.nil? # no leader yet
        Log.warn { "No leader available" }
        next
      end
      if uri == @advertised_uri # if this instance has become leader
        select
        when elected.when_true.receive
          Log.debug { "Elected leader, don't replicate from self" }
          return
        when timeout(1.second)
          raise Error.new("Another node in the cluster is advertising the same URI")
        end
      end
      return unless follow(uri, elected)
    end
  end

  # Starts replicating from the leader at *uri*. False if this node was
  # elected in the term of *elected* meanwhile, e.g. while waiting for the
  # secret.
  private def follow(uri : String, elected : BoolChannel) : Bool
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
    # Only made when it's used: it opens the data dir and takes over
    # checksums.sha1
    r = @follow_lock.synchronize do
      next if elected.value || @stopping.get
      client = Clustering::Client.new(@config, @id, secret)
      client.on_host_change = ->leader_host_changed
      @repli_client.set(client)
      client
    end
    return false unless r
    spawn r.follow(uri), name: "Clustering client #{uri}"
    SystemD.notify_ready
    true
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
    begin
      @lease.try &.release
    rescue ex
      Log.warn(exception: ex) { "Failed to release lease while stepping down, it will expire on its own" }
    end
    exit 3
  end

  def wait_to_be_insync(lease)
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
        select
        when err = lease.expired.receive?
          return if err.nil? && @stopping.get # released by #stop
          if err
            Log.fatal { "Lease expired while waiting to be in sync: #{err.message}" }
          else
            Log.fatal { "Lease expired while waiting to be in sync" }
          end
          exit 3
        when in_sync.receive?
          Log.info { "In sync with leader" }
        end
      end
    end
  end
end
