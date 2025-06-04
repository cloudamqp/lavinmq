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
  end

  # This method is called by the Launcher#run.
  # The block will be yielded when the controller's prerequisites for a leader
  # to start are met, i.e when the current node has been elected leader.
  # The method is blocking.
  @is_leader = BoolChannel.new(false)

  def run(&)
    lease = @lease = @etcd.lease_grant(id: @id)
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync
    @etcd.election_campaign("#{@config.clustering_etcd_prefix}/leader", @advertised_uri, lease: @id) # blocks until becoming leader
    @is_leader.set(true)
    @repli_client.try &.close
    # TODO: make sure we still are in the ISR set
    yield
    loop do
      lease.wait(30.seconds)
      GC.collect
    end
  rescue Etcd::Lease::Lost
    unless @stopped
      Log.fatal { "Lost cluster leadership" }
      exit 3
    end
  rescue Etcd::LeaseAlreadyExists
    Log.fatal { "Cluster ID #{@id.to_s(36)} used by another node" }
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
    if isr = @etcd.get("#{@config.clustering_etcd_prefix}/isr")
      unless isr.split(",").map(&.to_i(36)).includes?(@id)
        Log.info { "ISR: #{isr}" }
        Log.info { "Not in sync, waiting for a leader" }
        @etcd.watch("#{@config.clustering_etcd_prefix}/isr") do |value|
          break if value.try &.split(",").map(&.to_i(36)).includes?(@id)
        end
        Log.info { "In sync with leader" }
      end
    end
  end

  class Error < Exception; end
end
