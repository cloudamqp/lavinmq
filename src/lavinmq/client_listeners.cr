require "socket"
require "openssl"
require "./config"
require "./proxy_protocol"
require "./amqp/server"
require "./mqtt/server"
require "./http/http_server"

module LavinMQ
  # The client ports (AMQP, MQTT and HTTP, with TLS and on unix sockets),
  # bound once for the process. While this node serves, what they accept is
  # handed to the broker (#serve). Otherwise it's forwarded as is to the same
  # port on the leader, TLS included, so a client can connect to any node,
  # and the ports stay open through leadership changes.
  class ClientListeners
    Log = LavinMQ::Log.for "listeners"

    CONNECT_TIMEOUT = 5.seconds

    enum Kind
      AMQP
      MQTT
      HTTP
    end

    # *target_port* is where it's forwarded to on the leader, nil when there
    # is no such port (a unix socket without the plain TCP port)
    private record Listener, kind : Kind, server : Socket::Server,
      tls : OpenSSL::SSL::Context::Server?, target_port : Int32?

    # Where connections go while this node serves
    record Brokers, amqp : AMQP::Server, mqtt : MQTT::Server, http : HTTP::Server

    @listeners = Array(Listener).new
    @brokers : Brokers? = nil
    # Forwarded client connections, closed when this node starts to serve
    @forwarded = Set(IO).new
    @lock = Mutex.new
    @closed = false
    @listening = Atomic(Bool).new(false)

    # *leader_host* is where connections are forwarded while not serving, see
    # Runner#leader_host
    def initialize(@config : Config, @leader_host : Proc(String?))
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
        add kind, TCPServer.new(address, port), nil, port
      end
      if tls_port > 0 && tls
        add kind, TCPServer.new(address, tls_port), tls, tls_port
      end
      unless unix_path.empty?
        File.delete?(unix_path)
        server = UNIXServer.new(unix_path)
        File.chmod(unix_path, 0o666)
        # To the leader's plain TCP port, unix sockets are local only
        add kind, server, nil, port > 0 ? port : nil
      end
    end

    private def add(kind, server, tls, target_port) : Nil
      Log.info { "Bound #{kind} to #{server.local_address}#{" (TLS)" if tls}" }
      @listeners << Listener.new(kind, server, tls, target_port)
    end

    # Starts accepting. Until then connections wait in the listen backlog,
    # rather than being closed while neither serving nor knowing of a
    # leader. #serve also starts it.
    def listen : Nil
      return if @listening.swap(true)
      @listeners.each do |l|
        spawn(name: "#{l.kind} listener #{l.server.local_address}") { accept_loop(l) }
      end
    end

    # Hands what's accepted to *brokers* from now on, or forwards it to the
    # leader with nil. Connections forwarded to the old leader are closed when
    # this node starts to serve, so their clients reconnect to it.
    def serve(brokers : Brokers?) : Nil
      forwarded = @lock.synchronize do
        @brokers = brokers
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
        @forwarded.dup.tap { @forwarded.clear }
      end
      @listeners.each { |l| l.server.close rescue nil }
      forwarded.each { |io| io.close rescue nil }
    end

    private def accept_loop(l : Listener) : Nil
      while client = l.server.accept?
        dispatch(l, client)
      end
    rescue ex : IO::Error
      Log.error(exception: ex) { "#{l.kind} listener failed" } unless @closed
    end

    private def dispatch(l : Listener, io : IO) : Nil
      client = io.as(Socket)
      if brokers = @lock.synchronize { @brokers }
        case l.kind
        in .amqp? then brokers.amqp.accept(client, l.tls)
        in .mqtt? then brokers.mqtt.accept(client, l.tls)
        in .http? then brokers.http.accept(client, l.tls)
        end
      elsif host = @leader_host.call
        spawn(name: "Forward #{l.kind} to the leader") { forward(l, client, host) }
      else
        client.close # neither serving nor following
      end
    end

    private def forward(l : Listener, client : Socket, host : String) : Nil
      unless port = leader_port(l)
        Log.debug { "Closing a #{l.kind} connection to #{l.server.local_address}, no port to forward it to" }
        return client.close
      end
      return client.close unless track(client)
      target = TCPSocket.new(host, port, connect_timeout: CONNECT_TIMEOUT)
      set_socket_options(target)
      if client.is_a?(TCPSocket)
        set_socket_options(client)
        # The leader parses it from followers, also before TLS, but HTTP has
        # no place for it
        unless l.kind.http?
          header = ProxyProtocol::V1.new(client.remote_address, client.local_address)
          target.write_bytes header, IO::ByteFormat::NetworkEndian
        end
      end
      spawn(name: "Forward #{l.kind} from the leader") do
        IO.copy(target, client)
      rescue IO::Error
      ensure
        target.close rescue nil
        client.close rescue nil
      end
      IO.copy(client, target)
    rescue ex : IO::Error | Socket::Error
      Log.debug { "Forwarding a #{l.kind} connection to #{host}: #{ex.message}" }
    ensure
      target.try &.close rescue nil
      client.close rescue nil
      @lock.synchronize { @forwarded.delete(client) }
    end

    # The leader's port of the same kind and TLS as *l*, it uses the same
    # ports as this node
    private def leader_port(l : Listener) : Int32?
      l.target_port
    end

    # False if closed or serving meanwhile
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
      return if socket.remote_address.loopback?
      if keepalive = @config.tcp_keepalive
        socket.keepalive = true
        socket.tcp_keepalive_idle = keepalive[0]
        socket.tcp_keepalive_interval = keepalive[1]
        socket.tcp_keepalive_count = keepalive[2]
      end
    end
  end
end
