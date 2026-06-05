require "raft"
require "uri"
require "http/client"
require "http/headers"
require "json"
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

    def post_election_in_isr? : Bool
      isr = @server.isr
      isr.empty? || isr.includes?(@server.node_id)
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

      maybe_bootstrap_or_join

      wait_to_be_insync
      @server.is_leader.when_true.receive
      unless post_election_in_isr?
        Log.fatal { "Won raft election but not in ISR (#{@server.isr.to_a}); refusing to serve" }
        exit 3
      end
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

    private def maybe_bootstrap_or_join : Nil
      join_target_path = File.join(@config.data_dir, ".join_target")
      if File.exists?(join_target_path)
        leader_uri = File.read(join_target_path).strip
        Log.info { "Found .join_target — joining cluster at #{leader_uri}" }
        perform_join(leader_uri)
        File.delete(join_target_path)
        return
      end
      return unless @server.peers.empty?
      Log.info { "Fresh node with no peers — bootstrapping single-node cluster" }
      unless @server.bootstrap
        Log.warn { "Bootstrap rejected (node already has peers); continuing as follower" }
      end
    end

    JOIN_MAX_ATTEMPTS   = 30
    JOIN_RETRY_INTERVAL = 1.second

    def perform_join(leader_uri : String) : Nil
      uri = URI.parse(leader_uri)
      raise "invalid leader URI scheme: #{uri.scheme.inspect}" unless uri.scheme == "http" || uri.scheme == "https"
      path = "/raft/admin/add_server/#{@server.node_id}"
      body = {"address" => build_advertised_address}.to_json
      headers = ::HTTP::Headers{"Content-Type" => "application/json"}
      last_error = "unknown error"
      user = uri.user
      password = uri.password
      JOIN_MAX_ATTEMPTS.times do |attempt|
        client = ::HTTP::Client.new(uri)
        client.basic_auth(user, password) if user
        begin
          response = client.post(path, headers: headers, body: body)
          if response.status_code == 200
            Log.info { "Joined cluster via #{leader_uri} on attempt #{attempt + 1}" }
            return
          end
          last_error = "HTTP #{response.status_code} #{response.body}"
          Log.warn { "Join attempt #{attempt + 1}/#{JOIN_MAX_ATTEMPTS} to #{leader_uri} got #{last_error}" }
        rescue ex
          last_error = ex.message.to_s
          Log.warn { "Join attempt #{attempt + 1}/#{JOIN_MAX_ATTEMPTS} to #{leader_uri} failed: #{ex.message}" }
        ensure
          client.close rescue nil
        end
        sleep JOIN_RETRY_INTERVAL unless attempt == JOIN_MAX_ATTEMPTS - 1
      end
      raise "join exhausted #{JOIN_MAX_ATTEMPTS} attempts: #{last_error}"
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
