require "../etcd"
require "./controller"
require "./etcd_coordinator"

# Leader election and ISR storage in an external etcd cluster.
class LavinMQ::Clustering::EtcdController < LavinMQ::Clustering::Controller
  getter coordinator : EtcdCoordinator

  @lease : Etcd::Lease? = nil

  def self.new(config : Config)
    etcd = Etcd.new(config.clustering_etcd_endpoints)
    new(config, etcd, EtcdCoordinator.new(config, etcd))
  end

  def initialize(config : Config, @etcd : Etcd, @coordinator : EtcdCoordinator)
    super(config)
    @elected_leader = BoolChannel.new(false)
  end

  def run(&)
    lease = @lease = @etcd.lease_grant(id: @id)
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync(lease)
    @coordinator.campaign(@advertised_uri, @id) # blocks until becoming leader, captures the fencing token
    @elected_leader.set(true)
    ensure_in_isr!
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close
    yield
    loop do
      lease.wait(1.hour) # blocks until the lease expires (raises Expired)
    end
  rescue Etcd::Lease::Expired
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopping
      Log.fatal { "Lease expired, lost leadership" }
      exit 3
    end
  rescue Etcd::LeaseAlreadyExists
    Log.fatal { "Cluster ID #{@id.to_s(36)} used by another node" }
    exit 3
  rescue Etcd::LeaseNotFound
    Log.fatal { "Lease not found, etcd may have been reset" }
    exit 3
  end

  def stop
    @stopped = @stopping = true
    @repli_client.try &.close
    @lease.try &.release
  end

  # Listens for leader change events, until this node is elected
  private def follow_leader_changes
    @etcd.elect_listen("#{@config.clustering_etcd_prefix}/leader") do |uri|
      follow(uri) { clustering_secret }
      return if uri == @advertised_uri # elected, see #elected?
    end
  end

  # The election gives this node a moment to notice it won
  private def elected? : Bool
    select
    when @elected_leader.when_true.receive
      Log.debug { "Elected leader, don't replicate from self" }
      @elected_leader.close
      true
    when timeout(1.second)
      false
    end
  end

  private def clustering_secret : String
    key = "#{@config.clustering_etcd_prefix}/clustering_secret"
    secret = @etcd.get(key)
    until secret # the leader might not have had time to set the secret yet
      Log.debug { "Clustering secret is missing, watching for it" }
      @etcd.watch(key) do |value|
        secret = value
        break
      end
    end
    secret
  end

  # A queued election candidacy can outlive ISR membership: this node may have
  # been dropped from the ISR (lagging or disconnected replication) after it
  # campaigned, and etcd's election doesn't consult the ISR key. Serving as
  # leader while missing confirmed messages would lose them cluster-wide — the
  # in-sync nodes would full_sync from us and delete them. Exit instead: that
  # releases the lease, withdraws the candidacy and lets an in-sync candidate
  # win; on restart wait_to_be_insync blocks until this node is re-synced.
  private def ensure_in_isr! : Nil
    ensure_in_isr!(@coordinator.isr) do
      # Release the lease explicitly: the election key is bound to it, so this
      # revokes the just-won leadership at once instead of leaving the cluster
      # leaderless until the lease TTL expires.
      @lease.try &.release
    rescue ex
      Log.warn(exception: ex) { "Failed to release lease while stepping down, it will expire on its own" }
    end
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
