require "socket"
require "crypto/subtle"
require "openssl/hmac"
require "random/secure"
require "./messages"
require "digest/crc32"
require "./file_applier"
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

  # Two outbound connections per peer carry everything this node sends to it:
  # one for appends with payload, see #send, one for everything else, so that
  # heartbeats and votes never wait behind payload. What the peer sends back
  # arrives on the peer's own outbound connections. Both sides authenticate with an HMAC keyed with the shared
  # clustering password: the client over both sides' nonces and its own
  # clustering id and address, the server over both nonces and its id, so
  # something listening at a peer's address that doesn't know the password
  # isn't taken for that peer.
  #
  # Only one address per clustering id may be connected at a time: a node
  # moving to a new address is accepted once its old connections have closed
  # (keepalive drops dead ones within seconds), a copy of a data dir running
  # next to the original is refused while the original is connected.
  class TCPTransport < Transport
    Log = LavinMQ::Log.for "clustering.raft.transport"

    MAGIC      = "LMQRAFT4".to_slice
    NONCE_SIZE =  32
    MAC_SIZE   =  32 # HMAC-SHA256
    QUEUE_SIZE = 256
    # Appends with payload queued per peer, more than the leader has in
    # flight to it
    DATA_QUEUE_SIZE = 32
    # A data frame: an append, at most Codec::MAX_FRAME, and its payload
    MAX_DATA_FRAME = 64 << 20
    # Open files kept per data connection, to read payload from
    OPEN_FILES = 64

    # What a connection carries, told by the client after authenticating
    enum Kind : UInt8
      Control = 0
      Data    = 1
    end
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
    @data_outbound = Hash(String, Channel(Message)).new
    @lock = Mutex.new
    @inbound = Set(TCPSocket).new
    # Clustering ids connected to us: their address and number of connections
    @inbound_ids = Hash(Int32, Tuple(String, Int32)).new
    @server : TCPServer? = nil
    @closed = false

    # Its fibers, outbound and inbound connections alike, run in
    # *execution_context*, see RaftController.
    def initialize(@password : String, @id : Int32, @address : String, peers : Enumerable(String),
                   @handler : TransportEvent ->, @connect_timeout = 1.second, @write_timeout = 2.seconds,
                   @execution_context : Fiber::ExecutionContext = Fiber::ExecutionContext.current)
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
          @data_outbound.delete(peer).try &.close
          true
        end
        wanted.each do |peer|
          next if @outbound.has_key?(peer)
          ch = @outbound[peer] = Channel(Message).new(QUEUE_SIZE)
          spawn_outbound(peer, ch, Kind::Control)
          data = @data_outbound[peer] = Channel(Message).new(DATA_QUEUE_SIZE)
          spawn_outbound(peer, data, Kind::Data)
        end
      end
    end

    def listen(server : TCPServer) : Nil
      @server = server
      while socket = server.accept?
        spawn_inbound(socket)
      end
    end

    # Fibers are spawned through these methods so that each gets its own
    # arguments. A block capturing the loop variable directly would see it
    # reassigned by the next iteration before the fiber starts.
    private def spawn_outbound(peer : String, ch : Channel(Message), kind : Kind) : Nil
      @execution_context.spawn(name: "raft outbound #{kind} #{peer}") { outbound_loop(peer, ch, kind) }
    end

    private def spawn_inbound(socket : TCPSocket) : Nil
      @execution_context.spawn(name: "raft inbound #{socket.remote_address}") { inbound(socket) }
    end

    # Appends with payload go on the data connection, see
    # AppendEntries#payload
    def send(to : String, msg : Message) : Nil
      data = msg.is_a?(AppendEntries) && msg.payload
      ch = @lock.synchronize { data ? @data_outbound[to]? : @outbound[to]? } || return
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
        @data_outbound.each_value &.close
        # Or peers stay connected to a transport that discards what they send
        @inbound.each { |socket| socket.close rescue nil }
        @inbound.clear
      end
    end

    private def connect(peer : String) : TCPSocket
      host, _, port = peer.rpartition(':')
      host = host[1...-1] if host.starts_with?('[') && host.ends_with?(']') # IPv6
      socket = TCPSocket.new(host, port.to_i, connect_timeout: @connect_timeout)
      socket.sync = false
      socket.tcp_nodelay = true
      enable_keepalive(socket)
      socket.read_timeout = @connect_timeout
      socket.write_timeout = @write_timeout
      socket
    end

    private def outbound_loop(peer : String, ch : Channel(Message), kind : Kind) : Nil
      backoff = 50.milliseconds
      until @closed || ch.closed?
        connected = false
        files = PayloadFiles.new
        begin
          socket = connect(peer)
          begin
            id = authenticate_client(socket, kind)
            Log.debug { "Connected to #{peer} (#{id.to_s(36)}), #{kind}" }
            connected = true
            @handler.call Identified.new(peer, id) if kind.control?
            return unless send_queued(socket, ch, kind, files)
          ensure
            socket.close rescue nil
            files.close
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

    # Sends what's queued until the queue is closed (false) or the connection
    # fails (raises)
    private def send_queued(socket : TCPSocket, ch : Channel(Message), kind : Kind, files : PayloadFiles) : Bool
      closed_by_peer = watch_for_close(socket)
      frame = IO::Memory.new
      loop do
        select
        when msg = ch.receive?
          return false unless msg
          if kind.data? && msg.is_a?(AppendEntries)
            write_data_frame(socket, msg, frame, files)
          else
            write_frame(socket, msg, frame)
          end
          socket.flush
        when closed_by_peer.receive?
          raise IO::Error.new("Closed by peer")
        end
      end
    end

    # The peer never writes after the handshake, so a read returns once it has
    # closed the connection, e.g. when it restarted. Otherwise that would only
    # be noticed by losing the next message, and the peer wouldn't see us as
    # connected meanwhile.
    private def watch_for_close(socket : TCPSocket) : Channel(Nil)
      closed = Channel(Nil).new
      socket.read_timeout = nil
      @execution_context.spawn(name: "raft outbound close watch") do
        socket.read_byte
      rescue IO::Error | Socket::Error
      ensure
        closed.close
      end
      closed
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
        kind = Kind.from_value?(socket.read_byte || raise IO::EOFError.new) ||
               raise IO::Error.new("Unknown connection kind")
        # No read timeout: peers that aren't leader send each other nothing
        # between elections. Keepalive drops half-open connections instead.
        enable_keepalive(socket)
        socket.read_timeout = nil
        socket.read_buffering = true
        # Decoded straight from the socket's read buffer, a frame at a time
        frame = IO::Sized.new(socket, 0)
        loop do
          msg = kind.data? ? read_data_frame(socket, frame) : read_frame(socket, frame)
          @handler.call msg
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

    private def read_frame(socket : IO, frame : IO::Sized) : Message
      len = socket.read_bytes UInt32, Codec::Format
      raise IO::Error.new("Frame too large (#{len} bytes)") if len > Codec::MAX_FRAME
      decode(frame, len)
    end

    private def decode(frame : IO::Sized, len : UInt32) : Message
      frame.read_remaining = len.to_u64
      msg = Codec.decode(frame, len.to_i32)
      raise IO::Error.new("#{frame.read_remaining} bytes left in a #{len} byte frame") unless frame.read_remaining.zero?
      msg
    end

    # A data frame: `u32` frame length, `u32` message length, the append, its
    # entries' payload in order, and the payload's CRC32. Each entry gets its
    # part of the payload.
    private def read_data_frame(socket : IO, frame : IO::Sized) : AppendEntries
      total = socket.read_bytes UInt32, Codec::Format
      raise IO::Error.new("Data frame too large (#{total} bytes)") if total > MAX_DATA_FRAME
      len = socket.read_bytes UInt32, Codec::Format
      raise IO::Error.new("Frame too large (#{len} bytes)") if len > Codec::MAX_FRAME || len.to_u64 + 8 > total
      msg = decode(frame, len)
      raise IO::Error.new("A #{msg.class} on a data connection") unless msg.is_a?(AppendEntries)
      payload = Bytes.new(total - len - 8)
      socket.read_fully(payload)
      crc = socket.read_bytes UInt32, Codec::Format
      raise IO::Error.new("Payload checksum mismatch") unless Digest::CRC32.checksum(payload) == crc
      offset = 0i64
      entries = msg.entries.map do |e|
        size = e.payload_size
        next e if size.zero?
        raise IO::Error.new("Payload too short for the entries") if offset + size > payload.size
        part = payload[offset, size]
        offset += size
        e.copy_with(payload: part)
      end
      raise IO::Error.new("#{payload.size - offset} bytes of payload left over") unless offset == payload.size
      msg.copy_with(entries: entries)
    end

    # Writes an append with its payload, read from the leader's files, see
    # #read_data_frame. When a file is gone, or is another file than the one
    # written to (replaced, a new inode), the append isn't sent: the files
    # are checked before anything is written.
    private def write_data_frame(io : IO, msg : AppendEntries, frame : IO::Memory, files : PayloadFiles) : Nil
      frame.clear
      Codec.encode(msg, frame)
      if frame.size > Codec::MAX_FRAME
        Log.error { "Not sending an append of #{frame.size} bytes, more than a frame can be" }
        return
      end
      ranges = msg.payload || return
      payload = ranges.sum(&.len)
      opened = ranges.map { |r| files.open(r) || return payload_unavailable(r) }
      io.write_bytes (frame.size + payload + 8).to_u32, Codec::Format
      io.write_bytes frame.size.to_u32, Codec::Format
      io.write frame.to_slice
      crc = Digest::CRC32.initial
      ranges.each_with_index do |r, i|
        crc = files.copy(opened[i], r, io, crc)
      end
      io.write_bytes crc, Codec::Format
    end

    private def payload_unavailable(range : PayloadRange) : Nil
      Log.warn { "Not sending payload from #{range.path}, it's gone or replaced" }
    end

    # *frame* is the connection's encoding buffer, reused for every message.
    # A message larger than the peer accepts is dropped here, sending it would
    # only make the peer close the connection.
    private def write_frame(io : IO, msg : Message, frame : IO::Memory) : Nil
      frame.clear
      Codec.encode(msg, frame)
      if frame.size > Codec::MAX_FRAME
        Log.error { "Not sending a #{msg.class.name} of #{frame.size} bytes, more than a frame can be" }
        return
      end
      io.write_bytes frame.size.to_u32, Codec::Format
      io.write frame.to_slice
    end

    # Returns the client's clustering id and address.
    private def authenticate_server(socket : IO) : Tuple(Int32, String)
      nonce = Random::Secure.random_bytes(NONCE_SIZE)
      socket.write nonce
      magic = Bytes.new(MAGIC.size)
      socket.read_fully(magic)
      raise AuthError.new("Bad protocol header") unless magic == MAGIC
      client_nonce = Bytes.new(NONCE_SIZE)
      socket.read_fully(client_nonce)
      id = socket.read_bytes Int32, Codec::Format
      len = socket.read_bytes Int32, Codec::Format
      raise AuthError.new("Invalid address length #{len}") unless 0 < len <= MAX_ADDRESS
      address = socket.read_string(len)
      mac = Bytes.new(MAC_SIZE)
      socket.read_fully(mac)
      unless Crypto::Subtle.constant_time_compare(mac, client_mac(nonce, client_nonce, id, address))
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
        socket.write server_mac(client_nonce, nonce, @id)
      rescue ex
        release(id, address)
        raise ex
      end
      {id, address}
    end

    # Returns the server's clustering id, once it has shown that it knows the
    # password too. Then tells it what the connection carries.
    private def authenticate_client(socket : IO, kind : Kind? = nil) : Int32
      nonce = Bytes.new(NONCE_SIZE)
      socket.read_fully(nonce)
      client_nonce = Random::Secure.random_bytes(NONCE_SIZE)
      socket.write MAGIC
      socket.write client_nonce
      socket.write_bytes @id, Codec::Format
      Codec.write_str socket, @address
      socket.write client_mac(nonce, client_nonce, @id, @address)
      socket.flush
      case Reply.from_value?(socket.read_byte || raise IO::EOFError.new)
      when Reply::Ok
        id = socket.read_bytes Int32, Codec::Format
        mac = Bytes.new(MAC_SIZE)
        socket.read_fully(mac)
        unless Crypto::Subtle.constant_time_compare(mac, server_mac(client_nonce, nonce, id))
          raise AuthError.new("The peer doesn't know the clustering password")
        end
        if kind
          socket.write_byte kind.value
          socket.flush
        end
        id
      when Reply::SameId
        raise AuthError.new("The peer has the same clustering id as this node")
      when Reply::IdConnected
        raise AuthError.new("The peer is already connected with #{Codec.read_str(socket)} for this node's clustering id")
      else
        raise AuthError.new("Authentication rejected by peer, check clustering password")
      end
    end

    # The label keeps a client's MAC from being reflected as a server's, the
    # nonces bind both to this connection
    private def client_mac(server_nonce : Bytes, client_nonce : Bytes, id : Int32, address : String) : Bytes
      mac("client", server_nonce, client_nonce, id, address)
    end

    private def server_mac(client_nonce : Bytes, server_nonce : Bytes, id : Int32) : Bytes
      mac("server", server_nonce, client_nonce, id, "")
    end

    private def mac(label : String, server_nonce : Bytes, client_nonce : Bytes, id : Int32, address : String) : Bytes
      data = IO::Memory.new(label.bytesize + 2 * NONCE_SIZE + 4 + address.bytesize)
      data.write label.to_slice
      data.write server_nonce
      data.write client_nonce
      data.write_bytes id, Codec::Format
      data.write address.to_slice
      OpenSSL::HMAC.digest(:sha256, @password, data.to_slice)
    end

    class AuthError < Exception; end

    # The files a data connection reads payload from, kept open by path
    private class PayloadFiles
      @files = Hash(String, Tuple(File, UInt64)).new
      @buffer = Bytes.new(64 * 1024)

      # The file of *range*, if it's still the inode written to
      def open(range : PayloadRange) : File?
        if cached = @files[range.path]?
          return cached[0] if cached[1] == range.ino
          cached[0].close
          @files.delete(range.path)
        end
        close if @files.size >= OPEN_FILES
        file = File.open(range.path) rescue return
        ino = FileApplier.inode(file)
        unless ino == range.ino
          file.close
          return
        end
        @files[range.path] = {file, ino}
        file
      end

      # Copies *range* of *file* to *io*, returns the CRC32 updated with it
      def copy(file : File, range : PayloadRange, io : IO, crc : UInt32) : UInt32
        file.read_at(range.pos, range.len) do |src|
          remaining = range.len
          while remaining > 0
            n = src.read(@buffer[0, Math.min(remaining, @buffer.size).to_i])
            raise IO::Error.new("#{range.path} is shorter than the payload") if n.zero?
            slice = @buffer[0, n]
            crc = Digest::CRC32.update(slice, crc)
            io.write slice
            remaining -= n
          end
        end
        crc
      end

      def close : Nil
        @files.each_value(&.[0].close)
        @files.clear
      end
    end
  end
end
