require "./client"
require "./static_members"
require "./vr_coordinator"
require "./vr_state"

module LavinMQ
  module Clustering
    class VRController
      Log = LavinMQ::Log.for "clustering.vr_controller"

      HEARTBEAT_TIMEOUT = 3.seconds

      getter node_id : Int32
      getter members : StaticMembers
      getter state : VRState
      getter coordinator : VRCoordinator

      @client : Client?
      @stopped = false
      @stop_channel = Channel(Nil).new

      def initialize(@config : Config)
        @members = StaticMembers.parse(@config.clustering_members)
        advertised_uri = @config.clustering_advertised_uri ||
                         "tcp://#{System.hostname}:#{@config.clustering_port}"
        @node_id = @config.clustering_node_id || @members.derive_node_id(advertised_uri)
        unless @members.includes?(@node_id)
          raise StaticMembers::Error.new("clustering.node_id #{@node_id} is not in clustering.members")
        end
        @state = VRState.new(@config.data_dir)
        @coordinator = VRCoordinator.new(@config)
        Log.info { "VR node #{@node_id}, quorum #{@members.quorum_size}/#{@members.size}" }
      end

      def id : Int32
        @node_id
      end

      def run(&)
        loop do
          return if @stopped
          primary_id = @members.primary_id(@state.view)
          if primary_id == @node_id
            run_as_primary { yield }
            return
          end
          follow_primary(primary_id)
          return if @stopped
        end
      rescue ex : StaticMembers::Error | PasswordStore::Error | VRState::Error
        Log.fatal { ex.message }
        exit 3
      end

      def stop
        return if @stopped
        @stopped = true
        @client.try &.close
        @stop_channel.close
      end

      private def run_as_primary(&)
        @state.role = "primary"
        execute_shell_command(@config.clustering_on_leader_elected, "leader_elected")
        Log.info { "VR primary view=#{@state.view} node_id=#{@node_id}" }
        yield
        @stop_channel.receive?
      end

      private def follow_primary(primary_id : Int32) : Nil
        uri = @members.uri(primary_id)
        @state.role = "backup"
        Log.info { "VR backup view=#{@state.view} following primary #{primary_id} at #{uri}" }
        client = Client.new(@config, @node_id, @coordinator.password)
        @client = client
        spawn(name: "VR follower #{uri}") do
          client.follow(uri)
        end

        disconnected_since : Time::Instant? = nil
        loop do
          select
          when @stop_channel.receive?
            return
          when timeout(500.milliseconds)
            if client.connected?
              disconnected_since = nil
            else
              disconnected_since ||= Time.instant
              if Time.instant - disconnected_since > HEARTBEAT_TIMEOUT
                new_view = @state.advance_view!
                Log.warn { "VR primary #{primary_id} unavailable, starting view #{new_view}" }
                return
              end
            end
          end
        end
      ensure
        @client.try &.close
        @client = nil
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
    end
  end
end
