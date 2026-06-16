require "raft"
require "uri"
require "http/client"
require "http/headers"
require "json"
require "random/secure"
require "../clustering/client"
require "../clustering/elector"
require "../clustering/coordinator"
require "./server"
require "./cluster_command"
require "./bootstrap_decision"

module LavinMQ::Raft
  class Backend
    include Clustering::Elector
    include Clustering::Coordinator
    Log = LavinMQ::Log.for "raft.backend"

    getter server : Server
    getter transport : ::Raft::TCPTransport
    @repli_client : ::LavinMQ::Clustering::Client? = nil
    @leader_changes = ::Channel(UInt64?).new(8)
    @stopped = false

    def initialize(@config : Config)
      @transport = ::Raft::TCPTransport.new(
        listen_address: @config.clustering_bind,
        listen_port: @config.clustering_raft_port,
        data_dir: File.join(@config.data_dir, "raft-transport"),
      )
      @server = Server.new(
        data_dir: @config.data_dir,
        advertised_address: build_advertised_address,
        transport: @transport,
      )
    end

    def node_id : Int32
      @server.node_id
    end

    def update_isr(synced_node_ids : Enumerable(Int32)) : Nil
      @server.propose(ClusterCommand::SetIsr.new(synced_node_ids.to_set))
    end

    def password : String
      existing = @server.secret
      return existing unless existing.empty?
      new_secret = Random::Secure.base64(32)
      @server.propose(ClusterCommand::SetSecret.new(new_secret))
      deadline = Time.instant + 5.seconds
      while @server.secret.empty?
        raise "timed out waiting for SetSecret apply" if Time.instant > deadline
        sleep 1.millisecond
      end
      @server.secret
    end

    def advertised_address : String
      build_advertised_address
    end

    def in_isr? : Bool
      isr = @server.isr
      isr.empty? || isr.includes?(@server.node_id)
    end

    # Read-only /raft/status|log|metrics surface. Safe to mount without
    # authentication (e.g. on the metrics port).
    def status_handler : ::Raft::HTTP::StatusHandler
      ::Raft::HTTP::StatusHandler.new(@server.node, @transport, advertised_address)
    end

    # Mutating POST /raft/admin/* surface. Mount only behind authentication.
    # Hands the raw Node to the handler for now; mutations should be marshaled
    # through Server#run_on_tick once the handler accepts a thread-safe facade.
    def admin_handler : ::Raft::HTTP::AdminHandler
      ::Raft::HTTP::AdminHandler.new(@server.node, @transport)
    end

    def campaign(& : ->)
      @transport.start
      @server.start
      # Callback fires on the tick fiber; never block it. Enqueue
      # non-blocking — a dedicated fiber does the real follow-leader work.
      @server.on_leader_change do |id|
        select
        when @leader_changes.send(id)
        else
          # Channel full — drop; next change will reconcile.
        end
      end
      spawn(name: "raft.backend follow_leader") { follow_leader_loop }

      maybe_bootstrap_or_join

      wait_for_insync_leadership
      execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
      @repli_client.try &.close
      yield

      @server.is_leader.when_false.receive
      return if @stopped
      execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
      Log.fatal { "Lost cluster leadership" }
      exit 3
    end

    def stop : Nil
      @stopped = true
      @leader_changes.close
      @repli_client.try &.close
      @server.stop
      @transport.stop
    end

    # The boot action this node would take given its config and current raft
    # state. Pure (no I/O); exposed for testing the formation decision.
    def boot_action : BootstrapDecision::Action
      BootstrapDecision.decide(advertised_management_host, @config.seed_uris, !@server.peers.empty?)
    end

    private def maybe_bootstrap_or_join : Nil
      join_target_path = File.join(@config.data_dir, ".join_target")
      if File.exists?(join_target_path)
        leader_uri = File.read(join_target_path).strip
        Log.info { "Found .join_target — joining cluster at #{leader_uri}" }
        perform_join(leader_uri)
        File.delete(join_target_path)
        return
      end

      case boot_action
      in .resume?
        Log.info { "Existing raft state — resuming as a member" }
      in .bootstrap?
        Log.info { "Bootstrapping single-node cluster" }
        unless @server.bootstrap
          Log.warn { "Bootstrap rejected (node already has peers); continuing as follower" }
        end
      in .join?
        Log.info { "Joining via seed URIs: #{@config.seed_uris.map(&.to_s).join(", ")}" }
        join_via_seeds(@config.seed_uris)
      end
    end

    # Our own advertised host, used to find ourselves in the seed list.
    # Authoritative (from clustering_advertised_uri), never inferred from binds.
    private def advertised_management_host : String
      URI.parse(@config.clustering_advertised_uri || "tcp://#{System.hostname}").host || System.hostname
    end

    JOIN_MAX_ATTEMPTS   = 30
    JOIN_RETRY_INTERVAL = 1.second

    def perform_join(leader_uri : String) : Nil
      uri = URI.parse(leader_uri)
      raise "invalid leader URI scheme: #{uri.scheme.inspect}" unless uri.scheme == "http" || uri.scheme == "https"
      join_via_seeds([uri])
    end

    # Cycle the seed URIs, asking each to add us, until one accepts (HTTP 200).
    # A non-leader seed answers non-200; the lowest seed may still be starting.
    def join_via_seeds(seeds : Array(URI)) : Nil
      raise "no seed URIs to join" if seeds.empty?
      seeds.each do |uri|
        unless uri.scheme == "http" || uri.scheme == "https"
          raise "invalid seed URI scheme #{uri.scheme.inspect} in #{uri}"
        end
      end
      address = build_advertised_address
      last_error = "unknown error"
      JOIN_MAX_ATTEMPTS.times do |attempt|
        seeds.each do |uri|
          begin
            # The route and payload format are raft.cr's contract; AdminClient
            # keeps them in the shard. We own retry policy here.
            status = ::Raft::HTTP::AdminClient.add_server(uri, @server.node_id.to_u64, address)
            if status.ok?
              Log.info { "Joined cluster via #{uri} on attempt #{attempt + 1}" }
              return
            end
            last_error = "HTTP #{status.code} from #{uri}"
            Log.warn { "Join attempt #{attempt + 1}/#{JOIN_MAX_ATTEMPTS} to #{uri} got HTTP #{status.code}" }
          rescue ex
            last_error = "#{uri}: #{ex.message}"
            Log.warn { "Join attempt #{attempt + 1}/#{JOIN_MAX_ATTEMPTS} to #{uri} failed: #{ex.message}" }
          end
        end
        sleep JOIN_RETRY_INTERVAL unless attempt == JOIN_MAX_ATTEMPTS - 1
      end
      raise "join exhausted #{JOIN_MAX_ATTEMPTS} attempts: #{last_error}"
    end

    HANDOFF_RETRY_INTERVAL = 1.second

    # Block until this node is both raft leader and in ISR (an empty ISR —
    # fresh bootstrap — counts as in sync). Raft elects on log recency alone,
    # and the raft log holds only tiny metadata entries, so a node whose
    # *data* is out of sync can win the election; serving from it would lose
    # messages. Hand leadership to an in-sync voter instead and keep waiting.
    private def wait_for_insync_leadership : Nil
      isr_changed = @server.state_machine.isr_changed
      logged_waiting = false
      loop do
        if @server.is_leader.value
          return if in_isr?
          hand_off_leadership
          select
          when isr_changed.receive
          when @server.is_leader.when_false.receive
          when timeout(HANDOFF_RETRY_INTERVAL)
          end
        else
          if !in_isr? && !logged_waiting
            logged_waiting = true
            Log.info { "Not in sync, waiting for the leader to add us to ISR" }
          end
          select
          when @server.is_leader.when_true.receive
          when isr_changed.receive
          end
        end
      end
    end

    private def hand_off_leadership : Nil
      isr = @server.isr
      voters = @server.voters
      target = isr.find { |id| id != @server.node_id && voters.includes?(id.to_u64) }
      if target.nil?
        Log.error { "Raft leader but not in ISR #{isr.to_a} and no in-sync voter available; refusing to serve until the ISR recovers" }
      elsif @server.transfer_leadership(to: target)
        Log.warn { "Raft leader but not in ISR #{isr.to_a}; handing leadership to in-sync node #{target}" }
      else
        Log.warn { "Raft leader but not in ISR #{isr.to_a}; leadership transfer to #{target} rejected, retrying" }
      end
    end

    private def follow_leader_loop : Nil
      loop do
        id = @leader_changes.receive
        begin
          handle_leader_change(id)
        rescue ex
          # handle_leader_change can raise on transient conditions (e.g.
          # @coordinator.password timing out before SetSecret has applied,
          # or Clustering::Client construction failing). Swallow + log so
          # the fiber survives — otherwise it'd die and subsequent leader
          # changes would silently fill the buffered channel until they
          # get dropped, breaking replication failover for this node.
          Log.error(exception: ex) { "handle_leader_change failed for #{id}; will retry on next leader change" }
        end
      end
    rescue ::Channel::ClosedError
    end

    private def handle_leader_change(new_leader_id : UInt64?) : Nil
      @repli_client.try &.close
      @repli_client = nil
      return if new_leader_id.nil?
      return if new_leader_id == @server.node_id.to_u64
      data_uri = lookup_data_uri(new_leader_id)
      return unless data_uri
      @repli_client = client = ::LavinMQ::Clustering::Client.new(
        @config, @server.node_id, password, raft_backend: self)
      spawn(name: "Clustering client #{data_uri}") { client.follow(data_uri) }
    end

    private def lookup_data_uri(node_id : UInt64) : String?
      peer = @server.peers.find { |p| p.id == node_id }
      return if peer.nil? || peer.address.empty?
      _, _, data_addr = peer.address.partition(",")
      return nil if data_addr.empty?
      "tcp://#{data_addr}"
    end

    private def build_advertised_address : String
      uri = URI.parse(@config.clustering_advertised_uri || "tcp://#{System.hostname}:#{@config.clustering_port}")
      host = uri.host || System.hostname
      "#{host}:#{@config.clustering_raft_port},#{host}:#{@config.clustering_port}"
    end

    private def execute_shell_command(command : String, event : String)
      return if command.empty?
      Log.info { "Executing #{event} hook in background: #{command}" }
      spawn(name: "#{event} hook") do
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
  end
end
