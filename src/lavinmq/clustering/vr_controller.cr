require "./client"
require "./leader_hooks"
require "./static_members"
require "./vr_coordinator"
require "./vr_state"

module LavinMQ
  module Clustering
    class VRController
      include LeaderHooks

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
        elected_hook_started = false
        @state.role = "primary"
        Log.info { "VR primary view=#{@state.view} node_id=#{@node_id}" }
        yield
        return if @stopped
        run_leader_elected_hook
        elected_hook_started = true
        @stop_channel.receive?
      ensure
        run_leader_lost_hook if elected_hook_started
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
    end
  end
end
