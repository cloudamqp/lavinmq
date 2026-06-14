require "./client"
require "./coordinator"
require "./etcd_coordinator"
require "./raft_coordinator"
require "../etcd"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32
  getter coordinator : Coordinator

  @repli_client : Client? = nil

  def self.new(config : Config)
    id = clustering_id(config)
    advertised_uri = advertised_uri(config)
    coordinator = build_coordinator(config, id, advertised_uri)
    new(config, coordinator, id, advertised_uri)
  end

  def initialize(@config : Config, @coordinator : Coordinator, @id : Int32, @advertised_uri : String)
    @elected_leader = BoolChannel.new(false)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  def run(&)
    @coordinator.start
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync
    @coordinator.await_leadership # blocks until becoming leader
    @elected_leader.set(true)
    ensure_in_isr!
    @coordinator.publish_leader_uri(@advertised_uri)
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close
    yield
    @coordinator.await_leadership_lost # blocks until leadership is lost
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopped
      Log.fatal { "Lost leadership" }
      exit 3
    end
  rescue Coordinator::IdConflict
    Log.fatal { "Cluster ID #{@id.to_s(36)} used by another node" }
    exit 3
  rescue Etcd::LeaseNotFound
    Log.fatal { "Lease not found, etcd may have been reset" }
    exit 3
  end

  @stopped = false

  def stop
    @stopped = true
    @repli_client.try &.close
    @coordinator.release
  end

  # Each node in a cluster has an unique id, for tracking ISR. With the raft
  # backend the id is assigned in config; otherwise it's read from (or generated
  # into) the data dir.
  private def self.clustering_id(config : Config) : Int32
    return config.clustering_raft_node_id if config.clustering_backend.raft?

    id_file_path = File.join(config.data_dir, ".clustering_id")
    begin
      File.read(id_file_path).to_i(36)
    rescue File::NotFoundError
      id = rand(Int32::MAX)
      Dir.mkdir_p config.data_dir
      File.write(id_file_path, id.to_s(36))
      Log.info { "Generated new clustering ID" }
      id
    end
  end

  private def self.advertised_uri(config : Config) : String
    config.clustering_advertised_uri ||
      "tcp://#{System.hostname}:#{config.clustering_port}"
  end

  private def self.build_coordinator(config : Config, id : Int32, advertised_uri : String) : Coordinator
    if config.clustering_backend.raft?
      RaftCoordinator.new(config, id, advertised_uri)
    else
      EtcdCoordinator.new(config, Etcd.new(config.clustering_etcd_endpoints), id, advertised_uri)
    end
  end

  # Replicate from the leader
  # Listens for leader change events
  private def follow_leader
    @coordinator.watch_leader_uri do |uri|
      if repli_client = @repli_client # is currently following a leader
        if repli_client.follows? uri
          next # if lost connection to the coordinator we continue follow the leader as is
        else
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
      secret = @coordinator.password
      @repli_client = r = Clustering::Client.new(@config, @id, secret)
      spawn r.follow(uri), name: "Clustering client #{uri}"
      SystemD.notify_ready
    end
  rescue ex : Error
    Log.fatal { ex.message }
    exit 36 # 36 for CF (Cluster Follower)
  rescue ex
    Log.fatal(exception: ex) { "Unhandled exception while following leader" }
    exit 36 # 36 for CF (Cluster Follower)
  end

  # A queued election candidacy can outlive ISR membership: this node may have
  # been dropped from the ISR (lagging or disconnected replication) after it
  # campaigned, and the election doesn't consult the ISR. Serving as leader
  # while missing confirmed messages would lose them cluster-wide — the in-sync
  # nodes would full_sync from us and delete them. Exit instead: that releases
  # leadership, withdraws the candidacy and lets an in-sync candidate win; on
  # restart wait_to_be_insync blocks until this node is re-synced.
  private def ensure_in_isr! : Nil
    isr = @coordinator.isr
    return if isr.nil? # no ISR recorded yet (fresh cluster)
    return if isr.includes?(@id)
    Log.fatal { "Won the leader election but is not in the in-sync replica set (ISR: #{isr.to_a}), stepping down" }
    # Release leadership explicitly so the cluster isn't left leaderless until a
    # timeout: revokes the just-won leadership at once.
    begin
      @coordinator.release
    rescue ex
      Log.warn(exception: ex) { "Failed to release leadership while stepping down" }
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
        select
        when @coordinator.membership_lost.receive?
          Log.fatal { "Lost membership while waiting to be in sync" }
          exit 3
        when in_sync.receive?
          Log.info { "In sync with leader" }
        end
      end
    end
  end

  private def execute_shell_command(command : String, event : String)
    return if command.empty?

    Log.info { "Executing #{event} hook in background: #{command}" }

    spawn name: "#{event} hook" do
      begin
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
  end

  class Error < Exception; end
end
