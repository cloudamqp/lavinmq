require "socket"
require "uri"
require "./protocol"

module LavinMQ
  module ClusteringV2
    # Manages an outbound RPC connection to a single cluster peer over plain TCP
    # authenticated via mutual HMAC-SHA256 challenge-response.
    class PeerSession
      getter local_node_id : UInt64
      getter local_peer_uri : String
      getter peer_id : UInt64
      getter execution_context : Fiber::ExecutionContext
      property peer_uri : String
      property? paused : Bool = false

      @secret : String
      @socket : TCPSocket?
      @inbound_sockets = Array(TCPSocket).new
      @write_lock = Mutex.new
      @connect_lock = Mutex.new
      @closed = false
      @on_frame : (UInt64, FrameType, IO) -> Nil

      def initialize(@local_node_id : UInt64,
                     @local_peer_uri : String,
                     @peer_id : UInt64,
                     @peer_uri : String,
                     @secret : String,
                     @execution_context : Fiber::ExecutionContext = Fiber::ExecutionContext::Concurrent.new("ClusteringV2-Peer-#{local_node_id}-#{peer_id}"),
                     &@on_frame : (UInt64, FrameType, IO) -> Nil)
      end

      def send_vote_request(req : VoteRequest) : Bool
        return false if @paused || @closed
        frame_type = req.pre_vote? ? FrameType::RequestPreVote : FrameType::RequestVote
        send_frame(frame_type) { |io| req.to_io(io) }
      end

      def send_vote_response(resp : VoteResponse) : Bool
        return false if @paused || @closed
        frame_type = resp.pre_vote? ? FrameType::RequestPreVoteResponse : FrameType::RequestVoteResponse
        send_frame(frame_type) { |io| resp.to_io(io) }
      end

      def send_append_entries(req : AppendEntriesRequest) : Bool
        return false if @paused || @closed
        send_frame(FrameType::AppendEntries) { |io| req.to_io(io) }
      end

      def send_append_entries_response(resp : AppendEntriesResponse) : Bool
        return false if @paused || @closed
        send_frame(FrameType::AppendEntriesResponse) { |io| resp.to_io(io) }
      end

      def send_install_snapshot(req : InstallSnapshotRequest) : Bool
        return false if @paused || @closed
        send_frame(FrameType::InstallSnapshot) { |io| req.to_io(io) }
      end

      def send_install_snapshot_response(resp : InstallSnapshotResponse) : Bool
        return false if @paused || @closed
        send_frame(FrameType::InstallSnapshotResponse) { |io| resp.to_io(io) }
      end

      def attach_inbound_socket(socket : TCPSocket) : Nil
        should_read = @write_lock.synchronize do
          if @closed || @paused
            socket.close rescue nil
            false
          else
            @inbound_sockets << socket
            if (current = @socket).nil? || current.closed?
              @socket = socket
            end
            true
          end
        end
        return unless should_read
        @execution_context.spawn(name: "ClusteringV2 peer reader #{@peer_id}") { read_loop(socket) }
      end

      def disconnect! : Nil
        @write_lock.synchronize do
          @socket.try &.close rescue nil
          @socket = nil
          @inbound_sockets.each { |s| s.close rescue nil }
          @inbound_sockets.clear
        end
      end

      def close : Nil
        @closed = true
        disconnect!
      end

      private def send_frame(type : FrameType, & : IO -> Nil) : Bool
        sock = ensure_connected || return false
        @write_lock.synchronize do
          return false if @closed || @paused
          if sock.closed?
            @socket = nil if @socket == sock
            return false
          end
          begin
            sock.write_byte(type.value)
            yield sock
            sock.flush
            true
          rescue IO::Error | Socket::Error
            sock.close rescue nil
            @socket = nil if @socket == sock
            false
          end
        end
      end

      private def active_socket : TCPSocket?
        @write_lock.synchronize do
          if sock = @socket
            return sock unless sock.closed?
            @socket = nil
          end
          nil
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def ensure_connected : TCPSocket?
        return if @closed || @paused
        if sock = active_socket
          return sock
        end

        @connect_lock.synchronize do
          return if @closed || @paused
          if sock = active_socket
            return sock
          end

          peer_uri = @peer_uri
          return if peer_uri.empty?

          dialed : TCPSocket? = nil
          begin
            uri = URI.parse(peer_uri)
            host = uri.host || "127.0.0.1"
            port = uri.port || 5679
            dialed = TCPSocket.new(host, port, dns_timeout: 1.second, connect_timeout: 1.second)
            dialed.tcp_nodelay = true
            dialed.sync = false
            server_id = Protocol.client_handshake!(dialed, @local_node_id, @secret, @local_peer_uri)
            if server_id != @peer_id
              dialed.close rescue nil
              return
            end

            spawn_reader = false
            chosen = @write_lock.synchronize do
              if @closed || @paused
                dialed.close rescue nil
                nil
              else
                if (existing = @socket) && !existing.closed? && !@inbound_sockets.includes?(existing)
                  existing.close rescue nil
                end
                @socket = dialed
                spawn_reader = true
                dialed
              end
            end

            if spawn_reader && (s = chosen)
              @execution_context.spawn(name: "ClusteringV2 peer reader #{@peer_id}") { read_loop(s) }
            end
            chosen
          rescue
            dialed.try &.close rescue nil
            nil
          end
        end
      end

      private def read_loop(sock : TCPSocket) : Nil
        loop do
          break if @closed || sock.closed?
          byte = sock.read_byte || break
          frame_type = FrameType.from_value?(byte) || break
          if @paused
            consume_ignored_frame(frame_type, sock)
            next
          end
          @on_frame.call(@peer_id, frame_type, sock)
        end
      rescue IO::Error | Socket::Error
      ensure
        @write_lock.synchronize do
          @inbound_sockets.delete(sock)
          if @socket == sock
            @socket = @inbound_sockets.find { |s| !s.closed? }
          end
        end
        sock.close rescue nil
      end

      private def consume_ignored_frame(frame_type : FrameType, io : IO) : Nil
        case frame_type
        in .request_pre_vote?, .request_vote?
          VoteRequest.from_io(io)
        in .request_pre_vote_response?, .request_vote_response?
          VoteResponse.from_io(io)
        in .append_entries?
          AppendEntriesRequest.from_io(io)
        in .append_entries_response?
          AppendEntriesResponse.from_io(io)
        in .install_snapshot?
          InstallSnapshotRequest.from_io(io)
        in .install_snapshot_response?
          InstallSnapshotResponse.from_io(io)
        in .handshake_hello?, .handshake_challenge?, .handshake_response?, .handshake_ack?
        end
      end
    end
  end
end
