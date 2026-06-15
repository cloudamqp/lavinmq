require "socket"
require "crypto/subtle"
require "../../clustering"
require "./messages"

module LavinMQ
  module Clustering
    module VR
      # One authenticated control-plane connection to a peer, carrying VR
      # Control messages (heartbeats, view-change). Separate from the data
      # replication connection so control liveness isn't coupled to replication
      # backpressure. The handshake mirrors the data path: an 8-byte header, a
      # shared-secret exchange, then the dialer's node id.
      class ControlSession
        getter peer_id : Int32

        # Outbound messages are queued and written by a dedicated fiber so that
        # sending NEVER blocks the caller: the VR::Node sends while holding its
        # FSM lock, and a blocking write to a slow/dead peer would otherwise
        # deadlock the whole cluster (every node stuck writing to every other).
        # The queue is bounded and drops on overflow — control messages are
        # periodic (heartbeats) or resent each tick (view-change), so a dropped
        # message is recovered by the next one.
        @outbound = ::Channel(Control::Message).new(256)

        def initialize(@socket : TCPSocket, @peer_id : Int32)
          @socket.sync = true
          @socket.read_buffering = true
          @socket.tcp_nodelay = true
        end

        # Start the writer fiber. Called by the mesh once the session is
        # registered (and by the handshake helpers so a returned session is
        # immediately usable).
        def start_writer : Nil
          spawn(name: "VR control writer #{@peer_id.to_s(36)}") { writer_loop }
        end

        private def writer_loop : Nil
          while msg = @outbound.receive?
            msg.to_io(@socket)
            @socket.flush
          end
        rescue IO::Error | Socket::Error
          # peer gone; the read loop will clean the session up
        end

        # Bound the handshake reads so a half-open or stuck peer (e.g. two nodes
        # connecting to a third at once at startup, where one accept is delayed)
        # fails fast and the dial loop retries, rather than blocking past the
        # view-change timeout.
        HANDSHAKE_TIMEOUT = 500.milliseconds

        # Dialer side: open a session to a peer whose id we already know (from
        # the roster). Sends header + secret + our id. Returns nil if the header
        # is rejected or authentication fails.
        def self.dial(socket : TCPSocket, secret : String, self_id : Int32, peer_id : Int32) : ControlSession?
          socket.read_timeout = HANDSHAKE_TIMEOUT
          socket.write Control::HEADER
          socket.write_bytes secret.bytesize.to_u8, IO::ByteFormat::LittleEndian
          socket.write secret.to_slice
          socket.flush
          case socket.read_byte
          when 0 # authenticated
          when 1   then raise AuthenticationError.new
          when nil then raise IO::EOFError.new
          else          raise Error.new("Unexpected control auth response")
          end
          socket.write_bytes self_id, IO::ByteFormat::LittleEndian
          socket.flush
          socket.read_timeout = nil # authed peer: stream messages without a deadline
          session = new(socket, peer_id)
          session.start_writer
          session
        end

        # Acceptor side: validate an inbound control connection's header and
        # secret, then read the dialer's node id. Returns nil on a bad header or
        # wrong secret (the socket is left for the caller to close).
        def self.accept(socket : TCPSocket, secret : String) : ControlSession?
          socket.read_timeout = HANDSHAKE_TIMEOUT
          header = uninitialized UInt8[8]
          socket.read_fully(header.to_slice)
          return nil unless header.to_slice == Control::HEADER
          accept_after_header(socket, secret)
        end

        # Acceptor side when the 8-byte VRCTL header has already been read by the
        # shared clustering listener (which peeks it to route REPLI vs VRCTL).
        def self.accept_after_header(socket : TCPSocket, secret : String) : ControlSession?
          socket.read_timeout = HANDSHAKE_TIMEOUT
          len = socket.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
          client_secret = socket.read_string(len)
          unless Crypto::Subtle.constant_time_compare(secret, client_secret)
            socket.write_byte 1u8
            socket.flush
            return nil
          end
          socket.write_byte 0u8
          socket.flush
          peer_id = socket.read_bytes(Int32, IO::ByteFormat::LittleEndian)
          socket.read_timeout = nil # authed peer: stream messages without a deadline
          session = new(socket, peer_id)
          session.start_writer
          session
        end

        # Enqueue a message for the writer fiber. Never blocks; if the queue is
        # full (a wedged peer) the message is dropped — the next heartbeat / tick
        # resend recovers it.
        def send(msg : Control::Message) : Nil
          @outbound.try_send(msg)
        rescue ::Channel::ClosedError
        end

        # Read one message, blocking until it arrives. Raises IO::EOFError /
        # Socket::Error when the connection drops.
        def receive : Control::Message
          Control.read(@socket)
        end

        def close : Nil
          @outbound.close
          @socket.close
        rescue IO::Error
        end
      end
    end
  end
end
