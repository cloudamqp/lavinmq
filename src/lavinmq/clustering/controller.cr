require "../etcd"
require "./client"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32

  @repli_client : Client? = nil

  def self.new(config : Config)
    etcd = Etcd.new(config.clustering_etcd_endpoints)
    new(config, etcd)
  end

  def initialize(@config : Config, @etcd : Etcd)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
    @is_leader = BoolChannel.new(false)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.

  def run(&)
    lease = @lease = @etcd.lease_grant(id: @id)
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync
    @etcd.election_campaign("#{@config.clustering_etcd_prefix}/leader", @advertised_uri, lease: @id) # blocks until becoming leader
    @is_leader.set(true)
    @etcd.del("#{@config.clustering_etcd_prefix}/isr") # delete legacy ISR key (used up until v2.6.x)
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close
    # TODO: make sure we still are in the ISR set
    yield
    loop do
      lease.wait(30.seconds)
      GC.collect
    end
  rescue Etcd::Lease::Lost
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopped
      Log.fatal { "Lost cluster leadership" }
      exit 3
    end
  rescue Etcd::LeaseAlreadyExists
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
    @lease.try &.release
  end

  # Each node in a cluster has an unique id, for tracking ISR
  private def clustering_id : Int32
    id_file_path = File.join(@config.data_dir, ".clustering_id")
    begin
      id = File.read(id_file_path).to_i(36)
    rescue File::NotFoundError
      id = next_cluster_node_id
      Dir.mkdir_p @config.data_dir
      File.write(id_file_path, id.to_s(36))
      Log.info { "Generated new clustering ID: #{id.to_s(36)}" }
    end
    id
  end

  # Generate a unique sequential cluster node ID
  # Tries to create replica/<id>/insync key, incrementing until finding an unused ID
  private def next_cluster_node_id : Int32
    prefix = @config.clustering_etcd_prefix
    id = 1
    loop do
      key = "#{prefix}/replica/#{id.to_s(36)}/insync"
      return id if @etcd.put_new(key, "0")
      id += 1
    end
  end

  # Replicate from the leader
  # Listens for leader change events
  private def follow_leader
    @etcd.elect_listen("#{@config.clustering_etcd_prefix}/leader") do |uri|
      if repli_client = @repli_client # is currently following a leader
        if repli_client.follows? uri
          next # if lost connection to etcd we continue follow the leader as is
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
        when @is_leader.when_true.receive
          Log.debug { "Is leader, don't replicate from self" }
          @is_leader.close
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

  def wait_to_be_insync
    new_key = "#{@config.clustering_etcd_prefix}/replica/#{@id.to_s(36)}/insync"
    old_key = "#{@config.clustering_etcd_prefix}/isr"

    # Check if already in sync via new key format
    if @etcd.get(new_key) == "1"
      return
    end

    # Check if already in sync via old comma-separated format (used up until v2.6.x)
    legacy_isr_exists = false
    if isr = @etcd.get(old_key)
      legacy_isr_exists = true
      if isr.split(",").map(&.to_i(36)).includes?(@id)
        Log.info { "In sync via legacy ISR key" }
        return
      end
    end

    # Not in sync, need to wait
    Log.info { "Replica #{@id.to_s(36)} not in sync, waiting for leader" }
    insync = Channel(Nil).new

    # Watch new individual key format
    spawn(name: "watch new insync key") do
      @etcd.watch(new_key) do |value|
        if value == "1"
          insync.close
          break
        end
      end
    end

    # Watch old comma-separated ISR key format only if it exists (backwards compatibility
    # during rolling upgrades where old leader uses comma-separated format, used up until v2.6.x)
    if legacy_isr_exists
      spawn(name: "watch legacy isr key") do
        @etcd.watch(old_key) do |value|
          # Break if deleted (new leader took over) or if we're now in the ISR
          break if value.nil?
          if value.split(",").map(&.to_i(36)).includes?(@id)
            insync.close
            break
          end
        end
      end
    end

    insync.receive?
    Log.info { "In sync with leader" }
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
