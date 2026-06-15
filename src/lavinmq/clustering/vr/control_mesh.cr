require "uri"
require "socket"
require "./membership"
require "./control_session"
require "./messages"

module LavinMQ
  module Clustering
    module VR
      # A full mesh of authenticated control connections between cluster members,
      # carrying VR Control messages. Pure transport: it delivers inbound
      # messages on a channel for the VR::Node FSM to consume and lets the node
      # broadcast / target-send. It holds no protocol state itself.
      #
      # To keep exactly one connection per pair, each node DIALS peers with a
      # higher id and ACCEPTS connections from peers with a lower id, so every
      # pair has a single link usable in both directions.
      class ControlMesh
        Log = LavinMQ::Log.for "clustering.vr.mesh"

        # Inbound messages from any peer, consumed by the VR::Node FSM.
        getter inbound : ::Channel(Control::Message)

        # Retry quickly: a failed/slow handshake must be re-attempted well within
        # the view-change timeout so a startup connect race doesn't trigger a
        # spurious election.
        def initialize(@membership : Membership, @secret : String,
                       @reconnect_interval : Time::Span = 200.milliseconds,
                       inbound_capacity : Int32 = 1024)
          @self_id = @membership.self_id
          @sessions = Hash(Int32, ControlSession).new
          @lock = Mutex.new(:unchecked)
          @inbound = ::Channel(Control::Message).new(inbound_capacity)
          @closed = false
        end

        # Begin dialing the peers this node is responsible for (higher ids). Each
        # gets a fiber that connects and reconnects until the mesh is closed.
        def start : Nil
          @membership.peers.each do |peer|
            next unless peer.id > @self_id
            spawn(name: "VR dial #{peer.id.to_s(36)}") { dial_loop(peer) }
          end
        end

        private def dial_loop(peer : Member) : Nil
          uri = URI.parse(peer.uri)
          host = uri.host || raise Error.new("Control peer #{peer.id} has no host in #{peer.uri.inspect}")
          port = uri.port || raise Error.new("Control peer #{peer.id} has no port in #{peer.uri.inspect}")
          until @closed
            begin
              Log.debug { "node #{@self_id.to_s(36)} connecting to peer #{peer.id.to_s(36)} at #{host}:#{port}" }
              socket = TCPSocket.new(host, port, connect_timeout: 5.seconds)
              Log.debug { "node #{@self_id.to_s(36)} tcp-connected to peer #{peer.id.to_s(36)}, handshaking" }
              if session = ControlSession.dial(socket, @secret, @self_id, peer.id)
                Log.info { "node #{@self_id.to_s(36)} dialed peer #{peer.id.to_s(36)}" }
                register(session)
                read_loop(session)
              else
                socket.close
              end
            rescue ex : IO::Error | Socket::Error
              Log.debug { "Control dial to #{peer.id.to_s(36)} failed: #{ex.message}" }
            end
            sleep @reconnect_interval unless @closed
          end
        end

        # Handle an inbound control connection (a socket already accepted and
        # known to be VRCTL). Blocks reading from it until it drops, so the caller
        # should run this in its own fiber.
        def handle_accept(socket : TCPSocket) : Nil
          handle_session(ControlSession.accept(socket, @secret), socket)
        end

        # Handle an inbound control connection whose VRCTL header has already
        # been read by the shared clustering listener.
        def handle_accept_after_header(socket : TCPSocket) : Nil
          handle_session(ControlSession.accept_after_header(socket, @secret), socket)
        end

        private def handle_session(session : ControlSession?, socket : TCPSocket) : Nil
          if session
            Log.info { "node #{@self_id.to_s(36)} accepted peer #{session.peer_id.to_s(36)}" }
            register(session)
            read_loop(session)
          else
            socket.close
          end
        rescue ex : IO::Error | Socket::Error
          socket.close rescue nil
        end

        private def register(session : ControlSession) : Nil
          @lock.synchronize do
            if old = @sessions[session.peer_id]?
              old.close
            end
            @sessions[session.peer_id] = session
          end
        end

        private def read_loop(session : ControlSession) : Nil
          loop do
            @inbound.send(session.receive)
          end
        rescue ex : IO::Error | Socket::Error | ::Channel::ClosedError
          Log.debug { "node #{@self_id.to_s(36)} read_loop for peer #{session.peer_id.to_s(36)} ended: #{ex.class}" }
        ensure
          @lock.synchronize do
            # Only forget it if it's still the live session for this peer (a
            # reconnect may already have replaced it).
            @sessions.delete(session.peer_id) if @sessions[session.peer_id]?.same?(session)
          end
          session.close
        end

        # Send to every connected peer. Best-effort: a failing send just drops
        # that peer's session (its read_loop will clean up and, for a dialed
        # peer, reconnect).
        def broadcast(msg : Control::Message) : Nil
          @lock.synchronize { @sessions.values }.each do |session|
            session.send(msg)
          rescue IO::Error | Socket::Error
          end
        end

        # Send to one peer if connected; silently dropped otherwise.
        def send_to(peer_id : Int32, msg : Control::Message) : Nil
          session = @lock.synchronize { @sessions[peer_id]? }
          return unless session
          session.send(msg)
        rescue IO::Error | Socket::Error
        end

        # The set of peer ids currently connected (used for liveness / quorum
        # presence checks by the FSM).
        def connected_ids : Set(Int32)
          @lock.synchronize { @sessions.keys.to_set }
        end

        def close : Nil
          @closed = true
          @lock.synchronize do
            @sessions.each_value &.close
            @sessions.clear
          end
          @inbound.close
        end
      end
    end
  end
end
