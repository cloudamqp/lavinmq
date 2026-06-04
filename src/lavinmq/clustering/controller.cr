require "../etcd"
require "./client"

class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32

  # The downstream replication server. Set by the Launcher. In DR (relay) mode
  # the controller feeds this server from the upstream region instead of a local
  # message store.
  property replicator : Clustering::Server? = nil

  # Invoked when this region's role flips (primary <-> DR follower). The Launcher
  # sets it to gracefully stop the node's subsystems and then exit 38, so the
  # supervisor restarts into the re-derived role. Unset = a bare exit 38.
  property on_role_change : (-> Nil)? = nil

  # Invoked once when this node enters DR/relay mode (run_relay). The Launcher
  # sets it to start the relay's barebones services: AMQP/MQTT connection
  # rejecters and a minimal HTTP API (metrics + health + DR control).
  property on_relay_start : (-> Nil)? = nil

  # The client replicating from this region's own leader, managed solely by
  # the `follow_leader` monitor.
  @repli_client : Client? = nil
  # The client replicating from the foreign region's leader in DR/relay mode,
  # managed solely by `follow_foreign_leader`. Kept separate from
  # `@repli_client` so the local leader monitor (which closes `@repli_client`
  # on a local leader change) can never tear down the upstream relay link.
  @relay_client : Client? = nil

  # Readiness for a DR relay: true when the upstream relay client is connected
  # and past its initial sync (so the relay can serve its downstream followers).
  def relay_ready? : Bool
    @relay_client.try(&.connected?) || false
  end

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
    spawn(watch_upstream, name: "Upstream region watcher")
    wait_to_be_insync(lease)
    @etcd.election_campaign("#{@config.clustering_etcd_prefix}/leader", @advertised_uri, lease: @id) # blocks until becoming leader
    @is_leader.set(true)
    @repli_client.try &.close
    if upstream = effective_upstream.presence
      # DR (relay) mode: don't serve clients, replicate from the foreign region
      # and re-serve the stream to our own region's followers.
      run_relay(lease, upstream)
    else
      execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
      # TODO: make sure we still are in the ISR set
      yield
      loop do
        lease.wait(30.seconds)
        GC.collect
      end
    end
  rescue Etcd::Lease::Expired
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    unless @stopped
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

  @stopped = false

  def stop
    @stopped = true
    @repli_client.try &.close
    @relay_client.try &.close
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

  # Sentinel value for {prefix}/upstream_etcd that explicitly marks this region
  # as the primary, overriding the config fallback. Used to promote a DR region
  # durably (survives restart) — etcd's JSON omits empty values, so an empty
  # string is indistinguishable from an absent key and can't serve this purpose.
  UPSTREAM_PRIMARY = "primary"

  # The foreign region's etcd endpoints to replicate from, or "" when this
  # region is the primary. The {prefix}/upstream_etcd key in this region's own
  # etcd is authoritative; the config option is only a fallback for bootstrap.
  # Set the key to the foreign etcd endpoints to make this region a DR follower,
  # or to "primary" to promote it (failover).
  def effective_upstream : String
    upstream_from(@etcd.get(upstream_etcd_key))
  end

  private def upstream_from(value : String?) : String
    value ||= @config.clustering_upstream_etcd_endpoints
    value == UPSTREAM_PRIMARY ? "" : value
  end

  private def upstream_etcd_key : String
    "#{@config.clustering_etcd_prefix}/upstream_etcd"
  end

  # This node won its region's election while the region is a DR follower, so it
  # becomes the relay: it serves its own region's followers (downstream) and
  # replicates from the foreign region's leader (upstream), teeing the stream.
  private def run_relay(lease, upstream : String)
    replicator = @replicator
    unless replicator
      Log.fatal { "DR relay mode requires a clustering replicator" }
      exit 1
    end
    Log.info { "Region is a DR follower of #{upstream}" }
    replicator.relay_mode! # gate downstream full-syncs until the upstream sync completes
    start_downstream_listener(replicator)
    @on_relay_start.try &.call # start the relay's reject listeners + barebones HTTP API
    execute_shell_command(@config.clustering_on_demoted, "demoted")
    spawn(follow_foreign_leader(upstream, replicator), name: "Foreign leader monitor")
    SystemD.notify_ready
    loop do
      lease.wait(30.seconds)
      GC.collect
    end
  end

  private def start_downstream_listener(replicator : Clustering::Server)
    server = TCPServer.new(@config.clustering_bind, @config.clustering_port)
    spawn(replicator.listen(server), name: "Clustering listener")
  end

  # Watch the foreign region's etcd for its current leader and replicate from it,
  # forwarding the stream to our own region's followers via the replicator.
  private def follow_foreign_leader(upstream : String, replicator : Clustering::Server)
    prefix = @config.clustering_upstream_etcd_prefix.presence || @config.clustering_etcd_prefix
    # fail_fast: false so an unreachable foreign etcd raises (and we retry below)
    # instead of taking this DR region's relay process down with it.
    foreign = Etcd.new(upstream, fail_fast: false)
    loop do
      foreign.elect_listen("#{prefix}/leader") do |uri|
        if client = @relay_client
          if client.follows? uri
            next
          else
            client.close
          end
        end
        if uri.nil?
          Log.warn { "No upstream leader available" }
          next
        end
        Log.info { "Upstream leader: #{uri}" }
        key = "#{prefix}/clustering_secret"
        secret = foreign.get(key)
        until secret
          Log.debug { "Upstream clustering secret is missing, watching for it" }
          foreign.watch(key) do |value|
            secret = value
            break
          end
        end
        @relay_client = c = Clustering::Client.new(@config, @id, secret, proxy: false, relay: replicator)
        spawn c.follow(uri), name: "Relay client #{uri}"
      end
    rescue ex : Etcd::Error
      break if @stopped
      # The upstream region's etcd is unreachable. Keep the relay alive — its
      # existing @relay_client keeps streaming from the upstream leader over its
      # own TCP link — and retry watching for leader changes.
      Log.warn(exception: ex) { "Upstream etcd unreachable, retrying in 5s" }
      sleep 5.seconds
    end
  rescue ex
    return if @stopped # don't crash the foreign monitor during graceful shutdown
    Log.fatal(exception: ex) { "Unhandled exception while following upstream leader" }
    exit 36
  end

  # Restart (via the supervisor) when this region's role flips, so the node
  # re-derives whether it's a primary or a DR relay on the next boot. Failover =
  # clear {prefix}/upstream_etcd; reversal = set it to the new primary's etcd.
  private def watch_upstream
    dr_now = !effective_upstream.empty?
    @etcd.watch(upstream_etcd_key) do |value|
      dr_after = !upstream_from(value).empty?
      next if dr_after == dr_now
      if dr_after
        Log.fatal { "Region demoted to DR follower (upstream_etcd=#{value}); restarting to switch role" }
        execute_shell_command(@config.clustering_on_demoted, "demoted")
      else
        Log.fatal { "Region promoted to primary; restarting to switch role" }
        execute_shell_command(@config.clustering_on_promoted, "promoted")
      end
      # The Launcher installs a callback that gracefully stops the node's
      # subsystems before exiting 38; the supervisor restarts into the
      # re-derived role. Fall back to a bare exit when unset (e.g. specs).
      if cb = @on_role_change
        cb.call
      else
        exit 38 # 38 for region role change; supervisor restarts into the new role
      end
    end
  rescue ex
    Log.error(exception: ex) { "Unhandled exception while watching upstream region" } unless @stopped
  end

  def wait_to_be_insync(lease)
    if isr = @etcd.get("#{@config.clustering_etcd_prefix}/isr")
      unless isr.split(",").map(&.to_i(36)).includes?(@id)
        Log.info { "ISR: #{isr}" }
        Log.info { "Not in sync, waiting for a leader" }
        in_sync = Channel(Nil).new
        spawn do
          @etcd.watch("#{@config.clustering_etcd_prefix}/isr") do |value|
            if value.try &.split(",").map(&.to_i(36)).includes?(@id)
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
