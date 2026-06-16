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

      struct ControlResponse
        getter? granted : Bool
        getter view : Int64
        getter op_number : Int64

        def initialize(@granted : Bool, @view : Int64, @op_number : Int64)
        end
      end

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
            if win_election?
              run_as_primary { yield }
              return
            end
            new_view = @state.advance_view!
            Log.warn { "VR election failed for node #{@node_id} in view #{new_view - 1}, starting view #{new_view}" }
            next
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
        authority_lost = Channel(Int64).new(1)
        @state.role = "primary"
        Log.info { "VR primary view=#{@state.view} node_id=#{@node_id}" }
        yield
        return if @stopped
        run_leader_elected_hook
        elected_hook_started = true
        spawn(name: "VR authority monitor") { monitor_authority(authority_lost) }
        select
        when @stop_channel.receive?
        when higher_view = authority_lost.receive
          @state.observe_view!(higher_view)
          Log.fatal { "Lost VR majority authority in view #{@state.view}, stepping down" }
          exit 1
        end
      ensure
        run_leader_lost_hook if elected_hook_started
      end

      private def follow_primary(primary_id : Int32) : Nil
        uri = @members.uri(primary_id)
        @state.role = "backup"
        Log.info { "VR backup view=#{@state.view} following primary #{primary_id} at #{uri}" }
        client = Client.new(@config, @node_id, @coordinator.password, vr_state: @state)
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

      private def win_election? : Bool
        view = @state.view
        primary_id = @members.primary_id(view)
        return false unless primary_id == @node_id
        return false unless @state.grant_vote?(@node_id, view, @state.op_number, primary_id)

        votes = 1
        return true if votes >= @members.quorum_size

        peers = @members.ids.reject { |id| id == @node_id }
        responses = Channel(ControlResponse?).new(peers.size)
        peers.each do |peer_id|
          spawn(name: "VR vote request #{peer_id}") do
            responses.send(request_vote(peer_id, view, @state.op_number))
          end
        end

        peers.size.times do
          if response = responses.receive
            @state.observe_view!(response.view) if response.view > view
            votes += 1 if response.granted?
            return true if votes >= @members.quorum_size
          end
        end
        false
      end

      private def monitor_authority(authority_lost : Channel(Int64)) : Nil
        failed_since : Time::Instant? = nil
        view = @state.view
        loop do
          return if @stopped

          authorized, highest_view = authority_status(view)
          if highest_view > view
            authority_lost.try_send(highest_view)
            return
          end

          if authorized
            failed_since = nil
          else
            now = Time.instant
            failed_since ||= now
            if now - failed_since > HEARTBEAT_TIMEOUT
              authority_lost.try_send(view)
              return
            end
          end

          sleep 500.milliseconds
        end
      end

      private def authority_status(view : Int64) : {Bool, Int64}
        votes = 1
        highest_view = view
        return {true, highest_view} if votes >= @members.quorum_size

        peers = @members.ids.reject { |id| id == @node_id }
        responses = Channel(ControlResponse?).new(peers.size)
        peers.each do |peer_id|
          spawn(name: "VR authority request #{peer_id}") do
            responses.send(request_authority(peer_id, view))
          end
        end

        peers.size.times do
          if response = responses.receive
            highest_view = Math.max(highest_view, response.view)
            votes += 1 if response.granted?
          end
        end
        {votes >= @members.quorum_size, highest_view}
      end

      private def request_vote(peer_id : Int32, view : Int64, op_number : Int64) : ControlResponse?
        control_request(peer_id, ControlRequest::Vote) do |socket|
          socket.write_bytes view, IO::ByteFormat::LittleEndian
          socket.write_bytes @node_id, IO::ByteFormat::LittleEndian
          socket.write_bytes op_number, IO::ByteFormat::LittleEndian
        end
      end

      private def request_authority(peer_id : Int32, view : Int64) : ControlResponse?
        control_request(peer_id, ControlRequest::Authority) do |socket|
          socket.write_bytes view, IO::ByteFormat::LittleEndian
          socket.write_bytes @node_id, IO::ByteFormat::LittleEndian
        end
      end

      private def control_request(peer_id : Int32, request : ControlRequest, & : TCPSocket -> Nil) : ControlResponse?
        uri = @members.uri(peer_id)
        host = uri.hostname || raise StaticMembers::Error.new("Clustering member #{peer_id} is missing a host")
        socket = TCPSocket.new(host, uri.port || 5679)
        socket.sync = true
        socket.read_timeout = 1.second
        socket.write_timeout = 1.second
        socket.write StartV2
        password = @coordinator.password
        socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
        socket.write password.to_slice
        return unless socket.read_byte == 0

        socket.write_bytes @node_id, IO::ByteFormat::LittleEndian
        socket.write_byte ConnectionKind::Control.value
        socket.write_byte request.value
        yield socket
        socket.flush

        granted = socket.read_byte == 1
        response_view = socket.read_bytes Int64, IO::ByteFormat::LittleEndian
        response_op = socket.read_bytes Int64, IO::ByteFormat::LittleEndian
        ControlResponse.new(granted, response_view, response_op)
      rescue ex : IO::Error | Socket::Error | KeyError
        Log.debug(exception: ex) { "VR control #{request} to node #{peer_id} failed" }
        nil
      ensure
        socket.close if socket
      end
    end
  end
end
