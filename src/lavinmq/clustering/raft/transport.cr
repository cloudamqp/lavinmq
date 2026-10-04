require "socket"
require "crypto/subtle"
require "openssl/hmac"
require "random/secure"
require "./messages"
require "../../logger"

module LavinMQ::Clustering::Raft
  # A peer connected to us, advertising `address` as where to reach it
  record Connected, id : Int32, address : String
  # The peer's last connection to us closed
  record Disconnected, id : Int32, address : String
  # We connected to `address` and the node there has clustering id `id`
  record Identified, address : String, id : Int32

  alias TransportEvent = Message | Connected | Disconnected | Identified

  abstract class Transport
    # Best effort: may drop the message, raft retransmits.
    abstract def send(to : String, msg : Message) : Nil
    abstract def close : Nil

    # Connect to exactly these addresses from now on.
    def update_peers(addrs : Enumerable(String)) : Nil
    end

    # The clustering id of the node at `address`, nil if it can't be reached.
    def probe(address : String) : Int32?
    end
  end

  # One outbound connection per peer carries everything this node sends to
  # it; what the peer sends back arrives on the peer's own outbound
  # connection. Connections authenticate with an HMAC over a server nonce,
  # the client's clustering id and address, keyed with the shared clustering
  # password. The server answers with its own id.
  #
  # Only one address per clustering id may be connected at a time: a node
  # moving to a new address is accepted once its old connections have closed
  # (keepalive drops dead ones within seconds), a copy of a data dir running
  # next to the original is refused while the original is connected.
  class TCPTransport < Transport
    Log = LavinMQ::Log.for "clustering.raft.transport"

    MAGIC      = "LMQRAFT2".to_slice
    NONCE_SIZE =  32
    QUEUE_SIZE = 256
    # Seconds idle before probing, between probes, and unanswered probes
    # before a connection is dropped.
    KEEPALIVE   = {5, 1, 3}
    MAX_ADDRESS = 1024

    private enum Reply : UInt8
      BadPassword = 0
      Ok          = 1
      SameId      = 2
      IdConnected = 3
    end

    @outbound = Hash(String, Channel(Message)).new
    @lock = Mutex.new
    @inbound = Set(TCPSocket).new
    # Clustering ids connected to us: their address and number of connections
    @inbound_ids = Hash(Int32, Tuple(String, Int32)).new
    @server : TCPServer? = nil
    @closed = false

    def initialize(@password : String, @id : Int32, @address : String, peers : Enumerable(String),
                   @handler : TransportEvent ->, @connect_timeout = 1.second, @write_timeout = 2.seconds)
      update_peers(peers)
    end

    # Starts outbound connections to new addresses and closes those to
    # addresses that are no longer wanted.
    def update_peers(addrs : Enumerable(String)) : Nil
      wanted = addrs.to_set
      @lock.synchronize do
        return if @closed
        @outbound.reject! do |peer, ch|
          next false if wanted.includes?(peer)
          Log.debug { "Disconnecting from #{peer}" }
          ch.close
          true
        end
        wanted.each do |peer|
          next if @outbound.has_key?(peer)
          ch = @outbound[peer] = Channel(Message).new(QUEUE_SIZE)
          spawn(outbound_loop(peer, ch), name: "raft outbound #{peer}")
        end
      end
    end

    def listen(server : TCPServer) : Nil
      @server = server
      while socket = server.accept?
        spawn(inbound(socket), name: "raft inbound #{socket.remote_address}")
      end
    end

    def send(to : String, msg : Message) : Nil
      ch = @lock.synchronize { @outbound[to]? } || return
      select
      when ch.send(msg)
      else
        Log.debug { "Outbound queue to #{to} full, dropping #{msg.class}" }
      end
    rescue Channel::ClosedError
      # the peer was removed meanwhile
    end

    def probe(address : String) : Int32?
      socket = connect(address)
      begin
        authenticate_client(socket)
      ensure
        socket.close rescue nil
      end
    rescue ex : IO::Error | Socket::Error | AuthError | ArgumentError
      Log.warn { "Could not reach #{address}: #{ex.message}" }
      nil
    end

    def close : Nil
      @server.try &.close
      @lock.synchronize do
        @closed = true
        @outbound.each_value &.close
        # Or peers stay connected to a transport that discards what they send
        @inbound.each { |socket| socket.close rescue nil }
        @inbound.clear
      end
    end

    private def connect(peer : String) : TCPSocket
      host, _, port = peer.rpartition(':')
      socket = TCPSocket.new(host, port.to_i, connect_timeout: @connect_timeout)
      socket.sync = false
      socket.tcp_nodelay = true
      enable_keepalive(socket)
      socket.read_timeout = @connect_timeout
      socket.write_timeout = @write_timeout
      socket
    end

    private def outbound_loop(peer : String, ch : Channel(Message)) : Nil
      backoff = 50.milliseconds
      until @closed || ch.closed?
        connected = false
        begin
          socket = connect(peer)
          begin
            id = authenticate_client(socket)
            Log.debug { "Connected to #{peer} (#{id.to_s(36)})" }
            connected = true # ameba:disable Lint/UselessAssign (read in the rescue below)
            @handler.call Identified.new(peer, id)
            while msg = ch.receive?
              write_frame(socket, msg)
              socket.flush
            end
            return
          ensure
            socket.close rescue nil
          end
        rescue ex : IO::Error | Socket::Error | AuthError | ArgumentError
          return if @closed || ch.closed?
          if ex.is_a?(AuthError)
            Log.warn { "Connection to #{peer} refused: #{ex.message}" }
          else
            Log.debug { "Connection to #{peer} failed: #{ex.message}" }
          end
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
      @lock.synchronize do
        if @closed
          socket.close rescue nil
          return
        end
        @inbound << socket
      end
      socket.sync = true
      socket.read_timeout = @connect_timeout
      id, address = authenticate_server(socket)
      begin
        # No read timeout: peers that aren't leader send each other nothing
        # between elections. Keepalive drops half-open connections instead.
        enable_keepalive(socket)
        socket.read_timeout = nil
        socket.read_buffering = true
        loop do
          len = socket.read_bytes UInt32, Codec::Format
          raise IO::Error.new("Frame too large (#{len} bytes)") if len > Codec::MAX_FRAME
          buf = Bytes.new(len)
          socket.read_fully(buf)
          @handler.call Codec.decode(buf)
        end
      ensure
        release(id, address)
      end
    rescue ex : AuthError
      Log.warn { "Rejected raft connection from #{socket.remote_address rescue "?"}: #{ex.message}" }
    rescue ex : IO::Error | Socket::Error
      Log.debug { "Raft inbound connection closed: #{ex.message}" }
    ensure
      @lock.synchronize { @inbound.delete(socket) }
      socket.close rescue nil
    end

    # Registers a connection from `id` at `address`. Returns the address that
    # holds the id instead when it's another one. The first connection of an
    # id is reported as Connected, before any of its messages.
    private def claim(id : Int32, address : String) : String?
      first = false
      @lock.synchronize do
        if entry = @inbound_ids[id]?
          holder, count = entry
          return holder if holder != address
          @inbound_ids[id] = {address, count + 1}
        else
          @inbound_ids[id] = {address, 1}
          first = true
        end
      end
      @handler.call Connected.new(id, address) if first
      nil
    end

    private def release(id : Int32, address : String) : Nil
      last = false
      @lock.synchronize do
        _, count = @inbound_ids[id]? || return
        if count > 1
          @inbound_ids[id] = {address, count - 1}
        else
          @inbound_ids.delete(id)
          last = true
        end
      end
      @handler.call Disconnected.new(id, address) if last
    end

    private def enable_keepalive(socket : TCPSocket) : Nil
      socket.keepalive = true
      socket.tcp_keepalive_idle, socket.tcp_keepalive_interval, socket.tcp_keepalive_count = KEEPALIVE
    end

    private def write_frame(io : IO, msg : Message) : Nil
      bytes = Codec.encode(msg)
      io.write_bytes bytes.size.to_u32, Codec::Format
      io.write bytes
    end

    # Returns the client's clustering id and address.
    private def authenticate_server(socket : IO) : Tuple(Int32, String)
      nonce = Random::Secure.random_bytes(NONCE_SIZE)
      socket.write nonce
      magic = Bytes.new(MAGIC.size)
      socket.read_fully(magic)
      raise AuthError.new("Bad protocol header") unless magic == MAGIC
      id = socket.read_bytes Int32, Codec::Format
      len = socket.read_bytes Int32, Codec::Format
      raise AuthError.new("Invalid address length #{len}") unless 0 < len <= MAX_ADDRESS
      address = socket.read_string(len)
      mac = Bytes.new(32)
      socket.read_fully(mac)
      unless Crypto::Subtle.constant_time_compare(mac, hmac(nonce, id, address))
        socket.write_byte Reply::BadPassword.value
        raise AuthError.new("Bad password")
      end
      if id == @id
        socket.write_byte Reply::SameId.value
        raise AuthError.new("#{address} has the same clustering id as this node, " \
                            "delete .clustering_id on the node with a copied data dir")
      end
      if holder = claim(id, address)
        socket.write_byte Reply::IdConnected.value
        Codec.write_str socket, holder
        raise AuthError.new("#{address} has clustering id #{id.to_s(36)}, which #{holder} is connected with, " \
                            "delete .clustering_id on the node with a copied data dir")
      end
      begin
        socket.write_byte Reply::Ok.value
        socket.write_bytes @id, Codec::Format
      rescue ex
        release(id, address)
        raise ex
      end
      {id, address}
    end

    # Returns the server's clustering id.
    private def authenticate_client(socket : IO) : Int32
      nonce = Bytes.new(NONCE_SIZE)
      socket.read_fully(nonce)
      socket.write MAGIC
      socket.write_bytes @id, Codec::Format
      Codec.write_str socket, @address
      socket.write hmac(nonce, @id, @address)
      socket.flush
      case Reply.from_value?(socket.read_byte || raise IO::EOFError.new)
      when Reply::Ok
        socket.read_bytes Int32, Codec::Format
      when Reply::SameId
        raise AuthError.new("The peer has the same clustering id as this node")
      when Reply::IdConnected
        raise AuthError.new("The peer is already connected with #{Codec.read_str(socket)} for this node's clustering id")
      else
        raise AuthError.new("Authentication rejected by peer, check clustering password")
      end
    end

    private def hmac(nonce : Bytes, id : Int32, address : String) : Bytes
      data = IO::Memory.new(nonce.size + 4 + address.bytesize)
      data.write nonce
      data.write_bytes id, Codec::Format
      data.write address.to_slice
      OpenSSL::HMAC.digest(:sha256, @password, data.to_slice)
    end

    class AuthError < Exception; end
  end
end
