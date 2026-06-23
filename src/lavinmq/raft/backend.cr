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
require "./peer_address"
require "./bootstrap_decision"

module LavinMQ::Raft
  class Backend
    include Clustering::Elector
    include Clustering::Coordinator
    Log = LavinMQ::Log.for "raft.backend"

    getter server : Server
    getter transport : ::Raft::TCPTransport
    @repli_client : ::LavinMQ::Clustering::Client? = nil
    # @repli_client is mutated from three fibers — campaign (main), stop (signal
    # path) and reconcile_replication (the follow_leader fiber). Without
    # serialization a close can interleave with a rebuild, installing a client
    # nobody closes or leaving one following a stale leader after we became
    # leader. Every read/close/assign goes through this lock.
    @repli_client_lock = Mutex.new
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
      unless @server.propose_committed(ClusterCommand::SetIsr.new(synced_node_ids.to_set))
        Log.warn { "ISR update did not commit (leadership lost or overwritten); will retry on next sync" }
      end
    end

    PASSWORD_FILE = ".clustering_password"

    # The cluster's shared replication secret. It is read from a local file
    # (`<data_dir>/.clustering_password`), never the raft log — replicating it
    # through consensus would persist the secret in every node's log and
    # snapshots (and made a follower busy-wait on a SetSecret apply that could
    # time out and strand replication).
    #
    # A leader with no file yet generates one (single-node bootstrap); it must
    # be copied to every other node before they join. A node that is not the
    # leader and has no file cannot guess the secret, so it fails fast with an
    # actionable message rather than authenticating followers with the wrong
    # password. The follower file-sync skips this file (see Clustering::Client)
    # so replication can't delete it.
    def password : String
      path = File.join(@config.data_dir, PASSWORD_FILE)
      if File.exists?(path)
        existing = File.read(path).strip
        return existing unless existing.empty?
        # A 0-byte/whitespace-only file (a crash between File.open("w")
        # truncation and the write, a failed `openssl rand`, or an operator
        # `touch`) must NOT be used verbatim — a blank secret would authenticate
        # every follower. Treat it as missing: regenerate if we're the leader,
        # else fail fast just like an absent file.
        Log.warn { "Replication secret file is empty: #{path}; treating as missing" }
      end
      unless @server.is_leader.value
        Log.fatal { "Replication secret file missing or empty: #{path}. Copy it from another node in the cluster." }
        exit 3
      end
      secret = Random::Secure.base64(32)
      File.open(path, "w", perm: 0o600, &.print(secret))
      Log.info { "Generated clustering password at #{path}; copy it to every other node before they join" }
      secret
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
      # Wire the callbacks and start the reconcile loop BEFORE @server.start
      # (which spawns the tick loop): otherwise the tick loop can fire the first
      # nil->leader change into an unset callback and, since leader_id then stays
      # put, it never re-fires — and the follower never starts following.
      #
      # Both callbacks fire on the tick fiber; never block it. Enqueue
      # non-blocking — a dedicated fiber does the real reconcile work. A leader
      # change and a configuration change (a peer's address became known or
      # changed) both invalidate the current follow target, so both feed the
      # same reconcile loop.
      @server.on_leader_change { |id| enqueue_reconcile(id) }
      # The peers payload isn't needed here (addresses are cached in the Server);
      # resolve the current leader on the tick fiber — where this callback runs,
      # so the read is safe — and poke the reconcile loop with it.
      @server.on_configuration_change { |_peers| enqueue_reconcile(@server.leader_id) }
      spawn(name: "raft.backend follow_leader") { follow_leader_loop }

      @server.start

      maybe_bootstrap_or_join

      wait_for_insync_leadership
      return if @stopped
      execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
      close_repli_client # we're the leader now; stop following anyone
      yield

      # receive? (not receive): stop() closes is_leader, which would otherwise
      # raise Channel::ClosedError out of this (un-rescued, main-fiber) call.
      @server.is_leader.when_false.receive?
      return if @stopped
      execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
      Log.fatal { "Lost cluster leadership" }
      exit 3
    end

    def stop : Nil
      @stopped = true
      @leader_changes.close
      close_repli_client
      @server.stop
      @transport.stop
    end

    # Close and clear the replication client under the lock. Idempotent.
    private def close_repli_client : Nil
      @repli_client_lock.synchronize do
        @repli_client.try &.close
        @repli_client = nil
      end
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
        return unless with_join_retry { perform_join(leader_uri) } # interrupted by stop
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
        join_with_retry(@config.seed_uris)
      end
    end

    # Our own advertised host, used to find ourselves in the seed list.
    # Authoritative (from clustering_advertised_uri), never inferred from binds.
    private def advertised_management_host : String
      URI.parse(@config.clustering_advertised_uri || "tcp://#{System.hostname}").host || System.hostname
    end

    JOIN_MAX_ATTEMPTS   = 30
    JOIN_RETRY_INTERVAL = 1.second

    # Every seed was unreachable or still electing after a full sweep. Transient
    # by nature (the lowest-host bootstrapper may not be up yet), so the boot
    # path retries it rather than treating it as fatal. A malformed seed (bad
    # scheme / empty list) is a separate, permanent error and is NOT this type.
    class JoinExhausted < Exception; end

    def perform_join(leader_uri : String) : Nil
      uri = URI.parse(leader_uri)
      raise "invalid leader URI scheme: #{uri.scheme.inspect}" unless uri.scheme == "http" || uri.scheme == "https"
      join_via_seeds([uri])
    end

    # Retry a join sweep until it succeeds or stop() is called. join_via_seeds
    # raises JoinExhausted when every seed is unreachable or still electing; on
    # simultaneous boot the lowest-host bootstrapper may simply not be up yet,
    # so retry rather than let the exception escape campaign as an unhandled
    # backtrace. Returns true once joined, false if interrupted by stop().
    # A permanent error (bad scheme / empty list) is not JoinExhausted and
    # propagates.
    def join_with_retry(seeds : Array(URI), max_attempts : Int32 = JOIN_MAX_ATTEMPTS,
                        retry_interval : Time::Span = JOIN_RETRY_INTERVAL) : Bool
      with_join_retry(retry_interval) { join_via_seeds(seeds, max_attempts, retry_interval) }
    end

    private def with_join_retry(retry_interval : Time::Span = JOIN_RETRY_INTERVAL, & : -> Nil) : Bool
      until @stopped
        begin
          yield
          return true
        rescue ex : JoinExhausted
          return false if @stopped
          Log.warn { "#{ex.message}; retrying join sweep" }
          sleep retry_interval
        end
      end
      false
    end

    # Cycle the seed URIs, asking each to add us, until one accepts (HTTP 200).
    # A non-leader seed answers non-200; the lowest seed may still be starting.
    # Raises JoinExhausted after max_attempts full sweeps so the caller can
    # decide whether to retry (declarative boot) or surface it.
    def join_via_seeds(seeds : Array(URI), max_attempts : Int32 = JOIN_MAX_ATTEMPTS,
                       retry_interval : Time::Span = JOIN_RETRY_INTERVAL) : Nil
      raise "no seed URIs to join" if seeds.empty?
      seeds.each do |uri|
        unless uri.scheme == "http" || uri.scheme == "https"
          raise "invalid seed URI scheme #{uri.scheme.inspect} in #{uri}"
        end
      end
      address = build_advertised_address
      last_error = "unknown error"
      max_attempts.times do |attempt|
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
            Log.warn { "Join attempt #{attempt + 1}/#{max_attempts} to #{uri} got HTTP #{status.code}" }
          rescue ex
            last_error = "#{uri}: #{ex.message}"
            Log.warn { "Join attempt #{attempt + 1}/#{max_attempts} to #{uri} failed: #{ex.message}" }
          end
        end
        sleep retry_interval unless attempt == max_attempts - 1
      end
      raise JoinExhausted.new("join exhausted #{max_attempts} attempts: #{last_error}")
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
      # until @stopped + receive? on the is_leader arms: stop() sets @stopped
      # then closes is_leader, so the closed-channel wakeup falls through to the
      # loop guard and returns cleanly instead of raising Channel::ClosedError.
      until @stopped
        if @server.is_leader.value
          return if in_isr?
          hand_off_leadership
          select
          when isr_changed.receive
          when @server.is_leader.when_false.receive?
          when timeout(HANDOFF_RETRY_INTERVAL)
          end
        else
          if !in_isr? && !logged_waiting
            logged_waiting = true
            Log.info { "Not in sync, waiting for the leader to add us to ISR" }
          end
          select
          when @server.is_leader.when_true.receive?
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

    # Non-blocking enqueue from the tick fiber; the reconcile loop does the work.
    private def enqueue_reconcile(leader_id : UInt64?) : Nil
      select
      when @leader_changes.send(leader_id)
      else
        # Channel full — drop; the next change/config will reconcile.
      end
    end

    private def follow_leader_loop : Nil
      loop do
        id = @leader_changes.receive
        # Coalesce a burst (a contested election can queue X, Y, X) into a single
        # reconcile to the most recent observed leader, so we don't tear down and
        # rebuild the replication client for an intermediate candidate.
        id = Backend.drain_latest(@leader_changes, id)
        begin
          reconcile_replication(id)
        rescue ex
          # reconcile_replication can raise if Clustering::Client construction
          # fails. Swallow + log so the fiber survives — otherwise it'd die and
          # subsequent triggers would silently fill the buffered channel until
          # they get dropped, breaking replication failover for this node.
          Log.error(exception: ex) { "reconcile_replication failed for #{id}; will retry on next leader/config change" }
        end
      end
    rescue ::Channel::ClosedError
    end

    # Drain any already-queued triggers, returning the most recent (or `id` when
    # none are buffered). Collapses a burst of leader/config changes into one
    # reconcile.
    def self.drain_latest(channel : ::Channel(UInt64?), id : UInt64?) : UInt64?
      loop do
        select
        when newer = channel.receive
          id = newer
        else
          return id
        end
      end
    end

    enum LeaderChangeAction
      Keep          # leave the current replication client as-is
      StopFollowing # this node is the leader; follow no one
      Follow        # (re)build a client following the new leader
    end

    # Pure decision for a leader-change event. raft flips leader_id around
    # during contested elections (a higher-term RequestVote points it at a
    # candidate that may never win), so reacting to every change by tearing
    # down and rebuilding the replication client causes redundant proxy churn
    # and full re-syncs. Mirror the etcd backend: keep the current client on a
    # transient nil leader, when the new leader can't be resolved yet, or when
    # we already follow it; only rebuild for a genuinely different leader.
    def self.leader_change_action(new_leader_id : UInt64?, self_id : UInt64,
                                  data_uri : String?, already_following : Bool) : LeaderChangeAction
      return LeaderChangeAction::Keep if new_leader_id.nil?
      return LeaderChangeAction::StopFollowing if new_leader_id == self_id
      return LeaderChangeAction::Keep if data_uri.nil?
      return LeaderChangeAction::Keep if already_following
      LeaderChangeAction::Follow
    end

    # Reconcile the replication client to follow `new_leader_id` at its current
    # advertised address. Idempotent: keeps the client when already following
    # that leader, rebuilds it when the leader (or its address) changed, stops
    # when this node is the leader. Driven by both leader-change and
    # configuration-change events (see follow_leader_loop).
    private def reconcile_replication(new_leader_id : UInt64?) : Nil
      self_id = @server.node_id.to_u64

      # Only resolve an address when there is a different leader to follow.
      data_uri = nil
      if new_leader_id && new_leader_id != self_id
        data_uri = lookup_data_uri(new_leader_id)
      end

      # Hold the lock across the follows? read AND the swap so a concurrent
      # close (campaign/stop) can't interleave with a rebuild here.
      @repli_client_lock.synchronize do
        # Are we already replicating from that leader? If so, leave it alone.
        already_following = false
        if uri = data_uri
          already_following = @repli_client.try(&.follows?(uri)) || false
        end

        case Backend.leader_change_action(new_leader_id, self_id, data_uri, already_following)
        in .keep?
          # leave the current replication client untouched
        in .stop_following?
          @repli_client.try &.close
          @repli_client = nil
        in .follow?
          if uri = data_uri # always set when the action is Follow
            @repli_client.try &.close
            @repli_client = client = ::LavinMQ::Clustering::Client.new(
              @config, @server.node_id, password, raft_backend: self)
            spawn(name: "Clustering client #{uri}") { client.follow(uri) }
          end
        end
      end
    end

    private def lookup_data_uri(node_id : UInt64) : String?
      @server.peer_address(node_id).try &.data_uri
    end

    private def build_advertised_address : String
      uri = URI.parse(@config.clustering_advertised_uri || "tcp://#{System.hostname}:#{@config.clustering_port}")
      host = uri.host || System.hostname
      # Honor the port in clustering_advertised_uri for the data address (etcd
      # parity — a NAT/port-mapped deployment advertises its external data
      # port there); fall back to the local clustering_port when absent. The
      # raft-transport port has no advertised-URI field yet, so it uses the
      # local clustering_raft_port (correct for 1:1 port mapping).
      data_port = uri.port || @config.clustering_port
      PeerAddress.new(host, @config.clustering_raft_port, data_port).to_s
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
