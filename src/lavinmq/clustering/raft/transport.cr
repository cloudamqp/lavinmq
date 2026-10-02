require "socket"
require "crypto/subtle"
require "openssl/hmac"
require "random/secure"
require "./messages"
require "../../logger"

module LavinMQ::Clustering::Raft
  abstract class Transport
    # Best effort: may drop the message, raft retransmits.
    abstract def send(to : String, msg : Message) : Nil
    abstract def close : Nil
  end

  # One outbound connection per peer carries everything this node sends to
  # it; what the peer sends back arrives on the peer's own outbound
  # connection. Connections authenticate with an HMAC over a server nonce
  # keyed with the shared clustering password.
  class TCPTransport < Transport
    Log = LavinMQ::Log.for "clustering.raft.transport"

    MAGIC      = "LMQRAFT1".to_slice
    NONCE_SIZE =  32
    QUEUE_SIZE = 256

    @outbound = Hash(String, Channel(Message)).new
    @server : TCPServer? = nil
    @closed = false

    def initialize(@password : String, peers : Enumerable(String), @handler : Message ->,
                   @connect_timeout = 1.second, @write_timeout = 2.seconds)
      peers.each do |peer|
        ch = @outbound[peer] = Channel(Message).new(QUEUE_SIZE)
        spawn(outbound_loop(peer, ch), name: "raft outbound #{peer}")
      end
    end

    def listen(server : TCPServer) : Nil
      @server = server
      while socket = server.accept?
        spawn(inbound(socket), name: "raft inbound #{socket.remote_address}")
      end
    end

    def send(to : String, msg : Message) : Nil
      ch = @outbound[to]? || return
      select
      when ch.send(msg)
      else
        Log.debug { "Outbound queue to #{to} full, dropping #{msg.class}" }
      end
    end

    def close : Nil
      @closed = true
      @server.try &.close
      @outbound.each_value &.close
    end

    private def outbound_loop(peer : String, ch : Channel(Message)) : Nil
      backoff = 50.milliseconds
      until @closed
        connected = false
        begin
          host, _, port = peer.rpartition(':')
          socket = TCPSocket.new(host, port.to_i, connect_timeout: @connect_timeout)
          begin
            socket.sync = false
            socket.tcp_nodelay = true
            socket.read_timeout = @connect_timeout
            socket.write_timeout = @write_timeout
            authenticate_client(socket)
            Log.debug { "Connected to #{peer}" }
            connected = true # ameba:disable Lint/UselessAssign (read in the rescue below)
            while msg = ch.receive?
              write_frame(socket, msg)
              socket.flush
            end
            return
          ensure
            socket.close rescue nil
          end
        rescue ex : IO::Error | Socket::Error | AuthError
          return if @closed
          Log.debug { "Connection to #{peer} failed: #{ex.message}" }
          drain(ch)
          backoff = connected ? 50.milliseconds : Math.min(backoff * 2, 1.second)
          sleep backoff
        end
      end
    end

    # Messages queued while disconnected are stale by the time a connection
    # is up again; raft resends what still matters.
    private def drain(ch : Channel(Message)) : Nil
      loop do
        select
        when msg = ch.receive?
          return unless msg
        else
          return
        end
      end
    end

    private def inbound(socket : TCPSocket) : Nil
      socket.sync = true
      socket.read_timeout = @connect_timeout
      authenticate_server(socket)
      socket.read_timeout = nil
      socket.read_buffering = true
      loop do
        len = socket.read_bytes UInt32, Codec::Format
        raise IO::Error.new("Frame too large (#{len} bytes)") if len > Codec::MAX_FRAME
        buf = Bytes.new(len)
        socket.read_fully(buf)
        @handler.call Codec.decode(buf)
      end
    rescue ex : AuthError
      Log.warn { "Rejected raft connection from #{socket.remote_address rescue "?"}: #{ex.message}" }
    rescue ex : IO::Error | Socket::Error
      Log.debug { "Raft inbound connection closed: #{ex.message}" }
    ensure
      socket.close rescue nil
    end

    private def write_frame(io : IO, msg : Message) : Nil
      bytes = Codec.encode(msg)
      io.write_bytes bytes.size.to_u32, Codec::Format
      io.write bytes
    end

    private def authenticate_server(socket : IO) : Nil
      nonce = Random::Secure.random_bytes(NONCE_SIZE)
      socket.write nonce
      magic = Bytes.new(MAGIC.size)
      socket.read_fully(magic)
      raise AuthError.new("Bad protocol header") unless magic == MAGIC
      mac = Bytes.new(32)
      socket.read_fully(mac)
      unless Crypto::Subtle.constant_time_compare(mac, hmac(nonce))
        socket.write_byte 0u8
        raise AuthError.new("Bad password")
      end
      socket.write_byte 1u8
    end

    private def authenticate_client(socket : IO) : Nil
      nonce = Bytes.new(NONCE_SIZE)
      socket.read_fully(nonce)
      socket.write MAGIC
      socket.write hmac(nonce)
      socket.flush
      raise AuthError.new("Authentication rejected by peer, check clustering password") unless socket.read_byte == 1u8
    end

    private def hmac(nonce : Bytes) : Bytes
      OpenSSL::HMAC.digest(:sha256, @password, nonce)
    end

    class AuthError < Exception; end
  end
end
