require "random/secure"
require "../etcd"
require "./elector"
require "./coordinator"
require "./client"

class LavinMQ::Clustering::EtcdBackend
  include LavinMQ::Clustering::Elector
  include LavinMQ::Clustering::Coordinator
  Log = LavinMQ::Log.for "clustering.etcd_backend"

  getter node_id : Int32

  @repli_client : Client? = nil

  def self.new(config : Config)
    etcd = Etcd.new(config.clustering_etcd_endpoints)
    new(config, etcd)
  end

  def initialize(@config : Config, @etcd : Etcd)
    @node_id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
    @is_leader = BoolChannel.new(false)
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.

  def campaign(& : ->)
    # Every node ensures the clustering secret exists before campaigning or
    # following (put_or_get — first writer wins). If only the elected leader
    # wrote it, a follower could miss the leader's first write:
    # #follow_leader's get-then-watch loses a put that lands while the watch
    # stream is being established (Etcd#watch has no start_revision), leaving
    # the follower waiting for the secret forever.
    password
    lease = @lease = @etcd.lease_grant(id: @node_id)
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync(lease)
    @etcd.election_campaign("#{@config.clustering_etcd_prefix}/leader", @advertised_uri, lease: @node_id) # blocks until becoming leader
    @is_leader.set(true)
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close
    # TODO: make sure we still are in the ISR set
    yield
    loop do
      lease.wait(1.hour) # blocks until the lease expires (raises Expired)
    end
  rescue Etcd::Lease::Expired
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopped
      Log.fatal { "Lease expired, lost leadership" }
      exit 3
    end
  rescue Etcd::LeaseAlreadyExists
    Log.fatal { "Cluster ID #{@node_id.to_s(36)} used by another node" }
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

  def update_isr(synced_node_ids : Enumerable(Int32)) : Nil
    key = "#{@config.clustering_etcd_prefix}/isr"
    ids = synced_node_ids.map(&.to_s(36)).join(",")
    @etcd.put(key, ids)
  end

  def password : String
    key = "#{@config.clustering_etcd_prefix}/clustering_secret"
    secret = Random::Secure.base64(32)
    stored = @etcd.put_or_get(key, secret)
    Log.info { "Generated new clustering secret" } if stored == secret
    stored
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
      @repli_client = r = Clustering::Client.new(@config, @node_id, secret)
      spawn r.follow(uri), name: "Clustering client #{uri}"
    end
  rescue ex : Error
    Log.fatal { ex.message }
    exit 36 # 36 for CF (Cluster Follower)
  rescue ex
    Log.fatal(exception: ex) { "Unhandled exception while following leader" }
    exit 36 # 36 for CF (Cluster Follower)
  end

  def wait_to_be_insync(lease)
    if isr = @etcd.get("#{@config.clustering_etcd_prefix}/isr")
      unless isr.split(",").map(&.to_i(36)).includes?(@node_id)
        Log.info { "ISR: #{isr}" }
        Log.info { "Not in sync, waiting for a leader" }
        in_sync = Channel(Nil).new
        spawn do
          @etcd.watch("#{@config.clustering_etcd_prefix}/isr") do |value|
            if value.try &.split(",").map(&.to_i(36)).includes?(@node_id)
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
