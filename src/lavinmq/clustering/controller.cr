require "./client"
require "./server"
require "./vr/membership"
require "./vr/state"
require "./vr/control_mesh"
require "./vr/node"
require "./vr/coordinator"
require "./vr/messages"
require "../clustering"

# Drives clustering for this node using Viewstamped Replication. It owns the
# shared clustering listener (routing VRCTL control connections to the mesh and
# REPLI data connections to the replicator), the VR::Node consensus FSM, and the
# replication client used while this node is a backup. The interface the
# Launcher relies on — `run(&)`, `stop`, `id`, `replicator` — is unchanged.
class LavinMQ::Clustering::Controller
  Log = LavinMQ::Log.for "clustering.controller"

  getter id : Int32
  getter replicator : Clustering::Server
  @repli_client : Client? = nil
  @stopped = false
  @listener : TCPServer?
  @secret : String
  @membership : VR::Membership
  @coordinator : VR::Coordinator
  @mesh : VR::ControlMesh
  @node : VR::Node
  @state : VR::State
  # Re-pointing the replication client at a new primary is done by a dedicated
  # fiber, never inline in the VR::Node callback: `follow` is invoked from the
  # FSM while it holds its lock, and tearing down the old Client blocks (it waits
  # for the follower loop to drain). Blocking there would freeze heartbeat
  # processing and the election timer, so the node would time out and trigger a
  # spurious view change — a self-sustaining election storm at boot/failover.
  # Instead the callback just records the desired primary and wakes this fiber.
  @desired_primary : VR::Member? = nil
  @primary_now = false
  @follow_mu = Mutex.new(:unchecked)
  @follow_wake = ::Channel(Nil).new(1)

  def initialize(@config : Config)
    @membership = membership = VR::Membership.from_config(@config)
    # The node's id comes from the configured roster: it finds itself by matching
    # its advertised_uri, so the id is config-derived and stable — no separate
    # .clustering_id file needed.
    @id = membership.self_id
    @coordinator = coordinator = VR::Coordinator.new(@config)
    @secret = coordinator.password
    @replicator = replicator = Clustering::Server.new(@config, coordinator, @id, quorum: membership.quorum)
    @mesh = mesh = VR::ControlMesh.new(membership, @secret)
    @state = state = VR::State.load(@config.data_dir)
    @node = VR::Node.new(membership, mesh, state,
      heartbeat_interval: @config.clustering_heartbeat_interval_ms.milliseconds,
      view_change_timeout: @config.clustering_view_change_timeout_ms.milliseconds,
      # Election ordering uses the persisted durable op high-water (never ahead of
      # data on disk), so a freshly-restarted / under-replicated node reports its
      # true low position and cannot win and then overwrite a more-complete node.
      op_source: -> { state.op },
      # Non-blocking commit read: the FSM calls this while holding its lock (to
      # stamp heartbeats / election summaries), and the full #committed_op can
      # block on the replicator's lock during a follower full_sync — which would
      # freeze heartbeats and storm. The authoritative advance + persist still
      # runs in #committed_op on the confirm path.
      commit_source: -> { replicator.committed_op_cached },
      on_new_primary: ->(m : VR::Member) { follow(m) })
    replicator.vr_node = @node   # let the HTTP API surface clustering status
    replicator.vr_state = @state # leader seeds/persists its op high-water here
  end

  # Called by Launcher#run. Blocks until this node is elected primary, yields to
  # start the leader, then blocks until deposed (and exits so the supervisor
  # restarts it as a backup — there is no in-process demotion).
  def run(&)
    Log.info { "ID: #{@id.to_s(36)}, members: #{@membership.members.map(&.id.to_s(36))}" }
    start_listener
    spawn(name: "Clustering follow") { follow_loop }
    @mesh.start
    @node.start
    @node.wait_until_primary
    return if @stopped
    Log.info { "Elected primary" }
    @replicator.restore_checksums
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    # Stop following a previous leader: disable the follow fiber (we own the
    # AMQP/HTTP/MQTT ports as primary now) before tearing the client down.
    @follow_mu.synchronize { @primary_now = true; @desired_primary = nil }
    @repli_client.try &.close
    yield
    @node.wait_until_stepped_down
    return if @stopped
    on_leadership_lost
  end

  # Deposed as primary (a newer view exists, or we lost contact with a quorum).
  # Per the project's no-in-process-demotion decision, run the hook and exit so
  # the supervisor restarts this node as a backup. Overridable so a deposed node
  # can be observed in tests without killing the process.
  protected def on_leadership_lost : Nil
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    Log.fatal { "Lost leadership, stepping down" }
    exit 3
  end

  def stop
    @stopped = true
    @node.close
    @mesh.close
    @listener.try &.close
    @follow_wake.close
    @repli_client.try &.close
  end

  # The shared clustering listener: one TCP port for both VR control connections
  # and follower replication connections, distinguished by their start header.
  private def start_listener
    listener = @listener = TCPServer.new(@config.clustering_bind, @config.clustering_port)
    Log.info { "Clustering listening on #{listener.local_address}" }
    spawn(name: "Clustering listener") do
      while socket = listener.accept?
        spawn(name: "Clustering connection") { route(socket) }
      end
    end
  end

  private def route(socket : TCPSocket) : Nil
    socket.sync = true
    socket.read_buffering = true
    # Bound the header read so a peer that opens the connection but never sends
    # its 8-byte start header (a half-open/stuck dialer) can't wedge this accept
    # fiber forever. The header is the first thing both REPLI and VRCTL dialers
    # write, so a legitimate peer always meets this; the per-role handshake that
    # follows sets its own timeout.
    socket.read_timeout = 5.seconds
    header = uninitialized UInt8[8]
    socket.read_fully(header.to_slice)
    socket.read_timeout = nil
    case header.to_slice
    when VR::Control::HEADER
      @mesh.handle_accept_after_header(socket)
    when Clustering::Start
      # Only the current primary serves followers; a stray REPLI connection to a
      # backup is rejected (the follower will redial the real primary).
      if @node.primary?
        @replicator.accept_follower(socket)
      else
        socket.close
      end
    else
      Log.warn { "Unknown clustering start header from #{socket.remote_address}" }
      socket.close
    end
  rescue IO::Error
    socket.close rescue nil
  end

  # VR::Node callback (runs under the FSM lock): record the new primary and wake
  # the follow fiber. MUST NOT block — see @desired_primary above.
  private def follow(member : VR::Member) : Nil
    @follow_mu.synchronize { @desired_primary = member }
    @follow_wake.try_send(nil) rescue nil
  end

  # Applies the latest @desired_primary serially, off the FSM lock. Loops draining
  # the desired target so a burst of primary changes collapses to the last one.
  private def follow_loop : Nil
    loop do
      @follow_wake.receive
      until @stopped
        target = @follow_mu.synchronize do
          m = @desired_primary
          @desired_primary = nil
          m
        end
        break unless target
        apply_follow(target)
      end
    end
  rescue ::Channel::ClosedError
  end

  # Tear down any previous client and stream from the new primary, re-pointing the
  # AMQP/HTTP/MQTT proxies at it. Runs on the follow fiber only (so @repli_client
  # access is serialized) and may block on the old client's teardown — which is
  # why it is kept off the FSM lock.
  private def apply_follow(member : VR::Member) : Nil
    if client = @repli_client
      return if client.follows?(member.uri)
      client.close
    end
    # Bail if we've since been stopped or elected primary (the leader owns the
    # AMQP/HTTP/MQTT ports; starting a follower client would conflict on them).
    return if @stopped || @follow_mu.synchronize { @primary_now }
    Log.info { "Following primary #{member.id.to_s(36)} at #{member.uri}" }
    @repli_client = client = Clustering::Client.new(@config, @id, @secret)
    client.vr_state = @state # record the applied-op high-water as it syncs/acks
    # If this primary turns out to be behind our committed data, force a new
    # election rather than reconnecting to it forever (see Client#follow).
    client.on_behind_leader = -> { @node.request_view_change }
    spawn client.follow(member.uri), name: "Clustering client #{member.uri}"
    SystemD.notify_ready
  end

  private def execute_shell_command(command : String, event : String)
    return if command.empty?
    Log.info { "Executing #{event} hook in background: #{command}" }
    spawn name: "#{event} hook" do
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

  class Error < Exception; end
end
