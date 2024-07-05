require "../launcher"
require "../etcd"
require "./client"

class LavinMQ::Clustering::Controller
  Log = ::Log.for("clustering.controller")

  @id : Int32

  def initialize(@config = Config.instance)
    @etcd = Etcd.new(@config.clustering_etcd_endpoints)
    @id = clustering_id
    @advertised_uri = @config.clustering_advertised_uri ||
                      "tcp://#{System.hostname}:#{@config.clustering_port}"
  end

  def run
    spawn(follow_leader, name: "Follower monitor")
    wait_to_be_insync
    lease = @etcd.elect("#{@config.clustering_etcd_prefix}/leader", @advertised_uri) # blocks until becoming leader
    replicator = Clustering::Server.new(@config, @etcd)
    @launcher = l = Launcher.new(@config, replicator, lease)
    l.run
  end

  def stop
    @launcher.try &.stop
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
    end
    Log.info { "ID: #{id.to_s(36)}" }
    id
  end

  # Replicate from the leader
  # Listens for leader change events
  private def follow_leader
    repli_client = nil
    @etcd.elect_listen("#{@config.clustering_etcd_prefix}/leader") do |uri|
      next if repli_client.try &.follows?(uri) # if lost connection to etcd we continue follow the leader as is
      repli_client.try &.close
      if uri == @advertised_uri # if this instance has become leader
        Log.debug { "Is leader, don't replicate from self" }
        return
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
      repli_client = r = Clustering::Client.new(@config, @id, secret)
      spawn r.follow(uri), name: "Clustering client #{uri}"
    end
  end

  def wait_to_be_insync
    if isr = @etcd.get("#{@config.clustering_etcd_prefix}/isr")
      unless isr.split(",").map(&.to_i(36)).includes?(@id)
        Log.info { "Not in sync, waiting for a leader" }
        @etcd.watch("#{@config.clustering_etcd_prefix}/isr") do |value|
          break if value.try &.split(",").map(&.to_i(36)).includes?(@id)
        end
        Log.info { "In sync with leader" }
      end
    end
  end
end
