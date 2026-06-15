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

  def initialize(@config : Config)
    @membership = membership = VR::Membership.from_config(@config)
    @id = membership.self_id
    persist_clustering_id
    @coordinator = coordinator = VR::Coordinator.new(@config)
    @secret = coordinator.password
    @replicator = replicator = Clustering::Server.new(@config, coordinator, @id, quorum: membership.quorum)
    @mesh = mesh = VR::ControlMesh.new(membership, @secret)
    state = VR::State.load(@config.data_dir)
    @node = VR::Node.new(membership, mesh, state,
      heartbeat_interval: @config.clustering_heartbeat_interval_ms.milliseconds,
      view_change_timeout: @config.clustering_view_change_timeout_ms.milliseconds,
      op_source: -> { Math.max(replicator.current_op, @repli_client.try(&.applied_op) || 0u64) },
      on_new_primary: ->(m : VR::Member) { follow(m) })
  end

  # Called by Launcher#run. Blocks until this node is elected primary, yields to
  # start the leader, then blocks until deposed (and exits so the supervisor
  # restarts it as a backup — there is no in-process demotion).
  def run(&)
    Log.info { "ID: #{@id.to_s(36)}, members: #{@membership.members.map(&.id.to_s(36))}" }
    start_listener
    @mesh.start
    @node.start
    @node.wait_until_primary
    return if @stopped
    Log.info { "Elected primary" }
    @replicator.restore_checksums
    execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
    @repli_client.try &.close # stop following a previous leader
    yield
    @node.wait_until_stepped_down
    return if @stopped
    execute_shell_command(@config.clustering_on_leader_lost, "leader_lost")
    Log.fatal { "Lost leadership, stepping down" }
    exit 3
  end

  def stop
    @stopped = true
    @node.close
    @mesh.close
    @listener.try &.close
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
    header = uninitialized UInt8[8]
    socket.read_fully(header.to_slice)
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

  # Follow `member` as a backup: tear down any previous client and stream from
  # the new primary, re-pointing the AMQP/HTTP/MQTT proxies at it.
  private def follow(member : VR::Member) : Nil
    if client = @repli_client
      return if client.follows?(member.uri)
      client.close
    end
    Log.info { "Following primary #{member.id.to_s(36)} at #{member.uri}" }
    @repli_client = client = Clustering::Client.new(@config, @id, @secret)
    spawn client.follow(member.uri), name: "Clustering client #{member.uri}"
    SystemD.notify_ready
  end

  # Each node has a stable id (from the configured roster). Persist it for
  # continuity/debugging; refuse to run if a different id was recorded.
  private def persist_clustering_id
    path = File.join(@config.data_dir, ".clustering_id")
    Dir.mkdir_p @config.data_dir
    if File.exists?(path)
      existing = File.read(path).strip.to_i(36)
      if existing != @id
        Log.fatal { "Configured clustering id #{@id.to_s(36)} != recorded #{existing.to_s(36)} in #{path}" }
        exit 3
      end
    else
      File.write(path, @id.to_s(36))
    end
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
