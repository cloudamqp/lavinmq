require "socket"
require "openssl"
require "./config"
require "./proxy_protocol"
require "./amqp/server"
require "./mqtt/server"
require "./http/http_server"
require "./bool_channel"
require "./clustering/client"

module LavinMQ
  # The client ports (AMQP, MQTT and HTTP, with TLS and on unix sockets),
  # bound once for the process. While this node serves, what they accept is
  # handed to the broker (#serve). Otherwise it's forwarded to the leader, so
  # a client can connect to any node, and the ports stay open through
  # leadership changes. This node terminates TLS itself, with its own
  # certificate, and forwards the plaintext to the leader's plain port.
  class ClientListeners
    Log = LavinMQ::Log.for "listeners"

    CONNECT_TIMEOUT = 5.seconds

    enum Kind
      AMQP
      MQTT
      HTTP
    end

    # It's forwarded to the leader's port for *target_protocol* ("amqp",
    # "amqps" etc, see Clustering::Client::Leader), or to *target_port*, this
    # node's own port for it, if the leader doesn't advertise it. Nil when
    # there is no such port (a unix socket without the plain TCP port). With
    # *tls_passthrough* TLS is forwarded unterminated, to the leader's TLS
    # port, as there's no plain one to forward the plaintext to.
    private record Listener, kind : Kind, server : Socket::Server, address : String,
      tls : OpenSSL::SSL::Context::Server?, target_port : Int32?, tls_passthrough : Bool = false do
      def target_protocol : String
        tls_passthrough ? "#{kind.to_s.downcase}s" : kind.to_s.downcase
      end
    end

    # Where connections go while this node serves
    record Brokers, amqp : AMQP::Server, mqtt : MQTT::Server, http : HTTP::Server

    @listeners = Array(Listener).new
    @brokers : Brokers? = nil
    # Forwarded client connections, closed when this node starts to serve
    @forwarded = Set(IO).new
    @lock = Mutex.new
    @closed = false
    @listening = Atomic(Bool).new(false)
    # Open while what's accepted has somewhere to go, see #destination. The
    # accept loops wait for it, so that meanwhile connections wait in the
    # listen backlog instead of being closed. Also open once closed, for them
    # to see that.
    @accepting = BoolChannel.new(false)

    # *leader* is where connections are forwarded while not serving, see
    # Runner#leader
    def initialize(@config : Config, @leader : Proc(Clustering::Client::Leader?))
    end

    # Raises Socket::BindError if a port is in use
    def bind(amqp_tls : OpenSSL::SSL::Context::Server?, mqtt_tls : OpenSSL::SSL::Context::Server?,
             http_tls : OpenSSL::SSL::Context::Server?) : Nil
      c = @config
      bind(Kind::AMQP, c.amqp_bind, c.amqp_port, c.amqps_port, amqp_tls, c.unix_path)
      bind(Kind::MQTT, c.mqtt_bind, c.mqtt_port, c.mqtts_port, mqtt_tls, c.mqtt_unix_path)
      bind(Kind::HTTP, c.http_bind, c.http_port, c.https_port, http_tls, c.http_unix_path)
    end

    private def bind(kind, address, port, tls_port, tls, unix_path) : Nil
      if port > 0
        add kind, tcp_server(address, port), nil, port
      end
      if tls_port > 0 && tls
        # Terminated here and forwarded to the leader's plain port, as the
        # leader may not have a certificate for this node's name
        if port > 0
          add kind, tcp_server(address, tls_port), tls, port
        else
          add kind, tcp_server(address, tls_port), tls, tls_port, tls_passthrough: true
        end
      end
      unless unix_path.empty?
        File.delete?(unix_path)
        server = UNIXServer.new(unix_path)
        File.chmod(unix_path, 0o666)
        # To the leader's plain TCP port, unix sockets are local only
        add kind, server, nil, port > 0 ? port : nil
      end
    end

    # Accepted sockets inherit TCP_NODELAY from it, see HTTP.tcp_listener
    private def tcp_server(address, port) : TCPServer
      server = TCPServer.new(address, port)
      server.tcp_nodelay = @config.tcp_nodelay?
      server
    end

    private def add(kind, server, tls, target_port, tls_passthrough = false) : Nil
      address = server.local_address.to_s
      Log.info { "Bound #{kind} to #{address}#{" (TLS)" if tls}" }
      @listeners << Listener.new(kind, server, address, tls, target_port, tls_passthrough)
    end

    # Starts accepting, while this node serves or knows the leader, see
    # #leader_changed. #serve also starts it.
    def listen : Nil
      return if @listening.swap(true)
      destination
      @listeners.each do |l|
        spawn(name: "#{l.kind} listener #{l.address}") { accept_loop(l) }
      end
    end

    # Hands what's accepted to *brokers* from now on, or forwards it to the
    # leader with nil. Connections forwarded to the old leader are closed when
    # this node starts to serve, so their clients reconnect to it.
    def serve(brokers : Brokers?) : Nil
      forwarded = @lock.synchronize do
        @brokers = brokers
        destination_locked
        @forwarded.dup.tap { @forwarded.clear if brokers }
      end
      return unless brokers
      forwarded.each { |io| io.close rescue nil }
      listen
      @listeners.each do |l|
        case l.kind
        in .amqp? then brokers.amqp.adopt(l.server)
        in .mqtt? then brokers.mqtt.adopt(l.server)
        in .http?
        end
      end
    end

    def close : Nil
      forwarded = @lock.synchronize do
        @closed = true
        @brokers = nil
        destination_locked
        @forwarded.dup.tap { @forwarded.clear }
      end
      @listeners.each { |l| l.server.close rescue nil }
      forwarded.each { |io| io.close rescue nil }
    end

    # Called after what *leader* returns may have changed, see
    # Runner#on_leader_change
    def leader_changed : Nil
      destination
    end

    # Where what's accepted goes now, nil when nowhere. Opens or shuts
    # @accepting in the same step, so that a change notified meanwhile isn't
    # lost.
    private def destination : (Brokers | Clustering::Client::Leader)?
      @lock.synchronize { destination_locked }
    end

    private def destination_locked : (Brokers | Clustering::Client::Leader)?
      dest = @closed ? nil : (@brokers || @leader.call)
      open = @closed || !dest.nil?
      @accepting.set(open) unless @accepting.value == open
      dest
    end

    private def accept_loop(l : Listener) : Nil
      AcceptLoop.each(l.server, "#{l.kind} listener", @accepting) do |client|
        dispatch(l, client)
      end
    rescue ex : IO::Error
      Log.error(exception: ex) { "#{l.kind} listener failed" } unless @lock.synchronize { @closed }
    end

    private def dispatch(l : Listener, io : IO) : Nil
      client = io.as(Socket)
      loop do
        case dest = destination
        in Brokers
          case l.kind
          in .amqp? then dest.amqp.accept(client, l.tls)
          in .mqtt? then dest.mqtt.accept(client, l.tls)
          in .http? then dest.http.accept(client, l.tls)
          end
          return
        in Clustering::Client::Leader
          # Serving or closed meanwhile: it goes where things go now
          next unless track(client)
          leader = dest
          spawn(name: "Forward #{l.kind} to the leader") { forward(l, client, leader) }
          return
        in Nil
          # Accepted just as there stopped being anywhere to send it, it
          # waits like the ones in the backlog
          @accepting.when_true.receive
          return client.close if @lock.synchronize { @closed }
        end
      end
    end

    private def forward(l : Listener, client : Socket, leader : Clustering::Client::Leader) : Nil
      host = leader.host
      unless port = leader_port(l, leader)
        Log.debug { "Closing a #{l.kind} connection to #{l.address}, no port to forward it to" }
        return client.close
      end
      # A client that stops reading must not keep the connection open: the
      # copy from the leader would block in its write forever, and never see
      # that the leader closed the connection after its own tcp_send_timeout
      client.write_timeout = @config.tcp_send_timeout.seconds
      set_socket_options(client) if client.is_a?(TCPSocket)
      forward(l, client, host, port)
    rescue ex : OpenSSL::SSL::Error
      Log.debug { "TLS handshake with a #{l.kind} client on #{l.address} failed: #{ex.message}" }
    rescue ex : Socket::Error | ProxyProtocol::Error
      Log.warn { "Forwarding a #{l.kind} connection to #{host}: #{ex.message}" }
    rescue IO::Error | OpenSSL::Error
    rescue ex
      Log.warn { "Forwarding a #{l.kind} connection to #{host}: #{ex.inspect}" }
    ensure
      client.close rescue nil
      @lock.synchronize { @forwarded.delete(client) }
    end

    private def forward(l : Listener, client : Socket, host : String, port : Int32) : Nil
      # The leader parses it from followers, but HTTP has no place for it
      tcp_client = client.as?(TCPSocket) unless l.kind.http?
      # A load balancer's header comes before TLS
      inbound = inbound_proxy_header(tcp_client) if tcp_client
      downstream = terminate_tls(l, client)
      target = TCPSocket.new(host, port, connect_timeout: CONNECT_TIMEOUT)
      set_socket_options(target)
      if tcp_client
        target.write_bytes proxy_header(tcp_client, inbound, downstream), IO::ByteFormat::NetworkEndian
      end
      spawn(name: "Forward #{l.kind} from the leader") do
        IO.copy(target, downstream)
      rescue IO::Error | OpenSSL::Error
      ensure
        target.close rescue nil
        downstream.close rescue nil
      end
      IO.copy(downstream, target)
    ensure
      target.try &.close rescue nil
      downstream.try &.close rescue nil
    end

    # The leader reads a single PROXY header, so a header the client sent
    # (e.g. from a load balancer) can't be forwarded in front of ours. With
    # tcp_proxy_protocol it's consumed here, and forwarded instead of ours if
    # the source is trusted, like the leader would have done.
    private def inbound_proxy_header(client : TCPSocket) : (ProxyProtocol::V1 | ProxyProtocol::V2)?
      return unless @config.tcp_proxy_protocol?
      # Peeking for the header fills the read buffer, which would be the
      # default 32 KiB, kept for the life of the connection. The header
      # doesn't need more: the peek only matches the start of a signature, a
      # v1 header is at most 107 bytes and a v2 header is read with
      # read_fully. Reads are unbuffered once what's buffered is drained.
      client.buffer_size = 128
      ProxyProtocol.read_trusted(client, client.remote_address, @config, Log)
    end

    # The TLS session with the client, or the client itself when *l* is
    # plaintext or passes TLS through
    private def terminate_tls(l : Listener, client : Socket) : IO
      tls = l.tls
      return client if tls.nil? || l.tls_passthrough
      # A client that never completes the handshake mustn't hold the fiber
      client.read_timeout = ProxyProtocol::HANDSHAKE_TIMEOUT
      ssl = OpenSSL::SSL::Socket::Server.new(client, tls, sync_close: true)
      client.read_timeout = nil
      # IO.copy flushes nothing, and has its own buffer
      ssl.sync = true
      ssl.read_buffering = false
      ssl
    end

    # The load balancer's header, else one for the client. When this node
    # terminated TLS it's a v2 header with the TLS session in an SSL TLV, so
    # that the leader lists the connection as over TLS. A v2 header keeps its
    # other TLVs.
    private def proxy_header(client : TCPSocket, inbound, downstream : IO) : ProxyProtocol::V1 | ProxyProtocol::V2
      header = inbound || ProxyProtocol::V1.new(client.remote_address, client.local_address)
      return header unless downstream.is_a?(OpenSSL::SSL::Socket::Server)
      ssl = ProxyProtocol::V2.ssl_tlv(downstream.tls_version, downstream.cipher)
      if header.is_a?(ProxyProtocol::V2)
        tlvs = header.tlvs.reject(&.ssl?) << ssl
        if (src = header.src) && (dst = header.dst)
          ProxyProtocol::V2.new(src, dst, tlvs)
        else # UNSPEC addresses
          ProxyProtocol::V2.new(client.remote_address, client.local_address, tlvs)
        end
      else
        ProxyProtocol::V2.new(header.src, header.dst, [ssl])
      end
    end

    # The leader's port of the same kind and TLS as *l*: the one it
    # advertises, else the same as this node's
    private def leader_port(l : Listener, leader : Clustering::Client::Leader) : Int32?
      l.target_port.try { |own| leader.forward_port(l.target_protocol, own) }
    end

    # Tracks the TCP socket, closing it also ends a TLS session over it.
    # False if closed or serving meanwhile.
    private def track(client : IO) : Bool
      @lock.synchronize do
        return false if @closed || @brokers
        @forwarded << client
        true
      end
    end

    private def set_socket_options(socket : TCPSocket) : Nil
      socket.sync = true
      socket.read_buffering = false
      socket.tcp_nodelay = true
      @config.apply_tcp_keepalive(socket) unless socket.remote_address.loopback?
    end
  end
end
