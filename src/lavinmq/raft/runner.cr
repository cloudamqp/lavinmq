require "raft"
require "uri"
require "../clustering/client"
require "./server"
require "./coordinator"

module LavinMQ::Raft
  class Runner
    Log = LavinMQ::Log.for "raft.runner"

    getter server : Server
    getter coordinator : Coordinator
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
      @coordinator = Coordinator.new(@server)
    end

    def node_id : Int32
      @server.node_id
    end

    def advertised_address : String
      build_advertised_address
    end

    def run(&)
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
      spawn(name: "raft.runner follow_leader") { follow_leader_loop }

      maybe_bootstrap

      wait_to_be_insync
      @server.is_leader.when_true.receive
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

    private def maybe_bootstrap : Nil
      return unless @server.peers.empty?
      Log.info { "Fresh node with no peers — bootstrapping single-node cluster" }
      unless @server.bootstrap
        Log.warn { "Bootstrap rejected (node already has peers); continuing as follower" }
      end
    end

    private def wait_to_be_insync : Nil
      return if @server.isr.empty?
      return if @server.isr.includes?(@server.node_id)
      Log.info { "Not in sync, waiting for the leader to add us to ISR" }
      until @server.isr.includes?(@server.node_id)
        Fiber.yield
      end
      Log.info { "In sync with leader" }
    end

    private def follow_leader_loop : Nil
      loop do
        id = @leader_changes.receive
        handle_leader_change(id)
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
        @config, @server.node_id, @coordinator.password,
      )
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
