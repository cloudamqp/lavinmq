require "http/client"
require "http/server"
require "openssl"
require "socket"
require "uri"
require "../server"
require "../amqp/server"
require "../mqtt/server"
require "../http/http_server"
require "./protocol"
require "./raft_node"
require "./logical_replicator"

module LavinMQ
  module HTTP
    class Server
      def initialize(@server : LavinMQ::Server,
                     @amqp_server : LavinMQ::AMQP::Server,
                     @mqtt_server : LavinMQ::MQTT::Server,
                     prefix_handler : ::HTTP::Handler)
        oauth_authenticator =
          case auth = @server.authenticator
          when Auth::Chain
            auth.backends.select(Auth::OAuthAuthenticator).first?
          when Auth::OAuthAuthenticator
            auth
          end
        handlers = [
          prefix_handler,
          (::HTTP::LogHandler.new(log: Log) if Log.level == ::Log::Severity::Debug),
          StrictTransportSecurity.new,
          WebsocketProxy.new(@amqp_server, @mqtt_server),
          ViewsController.new,
          StaticController.new,
          oauth_authenticator && OAuthController.new(oauth_authenticator),
          AuthHandler.new(@server.authenticator, @server.users.direct_user, @internal_unix_socket_path),
          ApiErrorHandler.new,
          RequireUserHandler.new,
          PrometheusController.new(@server, require_authentication: true),
          ApiDefaultsHandler.new,
          MainController.new(@server, @amqp_server, @mqtt_server),
          DefinitionsController.new(@server),
          ConnectionsController.new(@server),
          ChannelsController.new(@server),
          ConsumersController.new(@server),
          ExchangesController.new(@server),
          QueuesController.new(@server),
          BindingsController.new(@server),
          VHostsController.new(@server),
          VHostLimitsController.new(@server),
          UsersController.new(@server),
          PermissionsController.new(@server),
          PermissionGroupsController.new(@server),
          ParametersController.new(@server),
          ShovelsController.new(@server),
          NodesController.new(@server),
          LogsController.new(@server),
        ].select(::HTTP::Handler)
        @http = ::HTTP::Server.new(handlers)
      end
    end
  end

  module AMQP
    class Server < ProtocolServer
      property leader_check : (-> Bool)?

      def handle_connection(socket, connection_info)
        if (check = @leader_check) && !check.call
          socket.close rescue nil
          return
        end
        if client = @connection_factory.create(socket, connection_info)
          vhost = client.vhost
          vhost.add_connection(client)
          begin
            client.run
          ensure
            vhost.rm_connection(client)
          end
        else
          socket.close
        end
      end
    end
  end

  module MQTT
    class Server < ProtocolServer
      property leader_check : (-> Bool)?

      def handle_connection(socket, connection_info)
        if (check = @leader_check) && !check.call
          socket.close rescue nil
          return
        end
        client = @connection_factory.start(socket, connection_info)
        socket.close if client.nil?
      end
    end
  end

  module ClusteringV2
    # HTTP Handler that serves read-only management queries (`GET /api/queues`,
    # `/api/exchanges`, `/api/vhosts`, `/api/overview`, `/metrics`, etc.)
    # directly from the Hot-Standby Follower's in-memory state, while
    # transparently reverse-proxying mutating requests (`POST`, `PUT`, `DELETE`)
    # and connection-local queries (`/api/connections`, `/api/channels`, `/api/consumers`)
    # to the current cluster leader.
    class FollowerHttpRouterHandler
      include ::HTTP::Handler

      getter local_served_count = Atomic(UInt64).new(0_u64)
      getter proxied_to_leader_count = Atomic(UInt64).new(0_u64)

      def initialize(@node : RaftNode)
      end

      def call(context : ::HTTP::Server::Context) : Nil
        if @node.promoted? || (@node.leader? && @node.wait_until_promoted(2.seconds))
          context.response.headers["X-LavinMQ-Cluster-Role"] = "leader"
          context.response.headers["X-LavinMQ-Node-Id"] = @node.node_id.to_s
          call_next(context)
          return
        end

        method = context.request.method
        path = context.request.path

        if FollowerHttpRouterHandler.serve_locally_on_follower?(method, path, context.request.headers)
          @local_served_count.add(1_u64, :relaxed)
          context.response.headers["X-LavinMQ-Cluster-Role"] = "follower"
          context.response.headers["X-LavinMQ-Node-Id"] = @node.node_id.to_s
          context.response.headers["X-LavinMQ-Leader-Id"] = @node.leader_id.to_s
          call_next(context)
        else
          @proxied_to_leader_count.add(1_u64, :relaxed)
          proxy_to_leader(context)
        end
      end

      def self.websocket_upgrade_request?(headers : ::HTTP::Headers?) : Bool
        return false unless hdrs = headers
        upgrade = hdrs["Upgrade"]? || return false
        connection = hdrs["Connection"]? || return false
        upgrade.compare("websocket", case_insensitive: true) == 0 &&
          connection.downcase.includes?("upgrade")
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def self.serve_locally_on_follower?(method : String, path : String, headers : ::HTTP::Headers? = nil) : Bool
        return false unless method == "GET" || method == "HEAD"
        return false if websocket_upgrade_request?(headers)
        return false if path == "/ws" || path.starts_with?("/ws/") || path == "/mqtt"

        # Connection, channel, and consumer listings reflect live TCP sockets on the leader
        return false if path == "/api/connections" || path.starts_with?("/api/connections/")
        return false if path.ends_with?("/connections")
        return false if path == "/api/channels" || path.starts_with?("/api/channels/")
        return false if path == "/api/consumers" || path.starts_with?("/api/consumers/")

        true
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def proxy_to_leader(context : ::HTTP::Server::Context) : Nil
        leader = @node.leader_member
        if leader.nil? || leader.node_id == @node.node_id || leader.http_uri.empty?
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print(%({"error":"Cluster leader currently unavailable"}))
          return
        end

        uri = URI.parse(leader.http_uri)
        host = uri.host || "127.0.0.1"
        port = uri.port || 15672

        if FollowerHttpRouterHandler.websocket_upgrade_request?(context.request.headers)
          proxy_websocket_to_leader(context, leader, uri, host, port)
          return
        end

        req_headers = ::HTTP::Headers.new
        context.request.headers.each do |k, values|
          next if k.compare("Host", case_insensitive: true) == 0
          next if k.compare("Connection", case_insensitive: true) == 0
          next if k.compare("Transfer-Encoding", case_insensitive: true) == 0
          values.each { |v| req_headers.add(k, v) }
        end

        body_str = context.request.body.try(&.gets_to_end)

        ::HTTP::Client.new(host, port, tls: uri.scheme == "https") do |client|
          client.connect_timeout = 2.seconds
          client.read_timeout = 5.seconds
          resp = client.exec(
            context.request.method,
            context.request.resource,
            headers: req_headers,
            body: body_str
          )
          context.response.status_code = resp.status_code
          resp.headers.each do |k, values|
            next if k.compare("Transfer-Encoding", case_insensitive: true) == 0
            next if k.compare("Connection", case_insensitive: true) == 0
            next if k.compare("Content-Length", case_insensitive: true) == 0
            values.each { |v| context.response.headers[k] = v }
          end
          context.response.headers["X-LavinMQ-Cluster-Role"] = "follower"
          context.response.headers["X-LavinMQ-Proxied-To-Leader"] = "true"
          context.response.headers["X-LavinMQ-Leader-Id"] = leader.node_id.to_s
          context.response.write(resp.body.to_slice)
        end
      rescue ex : IO::Error | Socket::Error
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print(%({"error":"Failed to proxy request to leader: #{ex.message}"}))
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def proxy_websocket_to_leader(context : ::HTTP::Server::Context,
                                            leader : MemberInfo,
                                            uri : URI,
                                            host : String,
                                            port : Int32) : Nil
        tcp_sock = TCPSocket.new(host, port, connect_timeout: 2.seconds)
        tcp_sock.tcp_nodelay = true
        tcp_sock.sync = true
        tcp_sock.read_buffering = false
        upstream : IO = if uri.scheme == "https"
          tls_ctx = OpenSSL::SSL::Context::Client.new
          tls_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
          ssl_sock = OpenSSL::SSL::Socket::Client.new(tcp_sock, context: tls_ctx, sync_close: true, hostname: host)
          ssl_sock.sync = true
          ssl_sock
        else
          tcp_sock
        end

        req_headers = ::HTTP::Headers.new
        context.request.headers.each do |k, values|
          next if k.compare("Host", case_insensitive: true) == 0
          values.each { |v| req_headers.add(k, v) }
        end
        req_headers["Host"] = "#{host}:#{port}"

        up_req = ::HTTP::Request.new(context.request.method, context.request.resource, headers: req_headers)
        up_req.to_io(upstream)
        upstream.flush

        resp = ::HTTP::Client::Response.from_io(upstream, ignore_body: true)
        context.response.status = resp.status
        resp.headers.each do |k, values|
          values.each { |v| context.response.headers.add(k, v) }
        end
        context.response.headers["X-LavinMQ-Cluster-Role"] = "follower"
        context.response.headers["X-LavinMQ-Proxied-To-Leader"] = "true"
        context.response.headers["X-LavinMQ-Leader-Id"] = leader.node_id.to_s

        if resp.status.switching_protocols?
          context.response.upgrade do |client_io|
            client_io.sync = true if client_io.responds_to?(:sync=)
            client_io.read_buffering = false if client_io.responds_to?(:read_buffering=)
            done = ::Channel(Nil).new(2)
            spawn(name: "ClusteringV2 ws proxy upstream->client") do
              IO.copy(upstream, client_io)
            rescue IO::Error
            ensure
              upstream.close rescue nil
              client_io.close rescue nil
              done.send(nil) rescue nil
            end
            begin
              IO.copy(client_io, upstream)
            rescue IO::Error
            ensure
              upstream.close rescue nil
              client_io.close rescue nil
              done.send(nil) rescue nil
            end
            2.times { done.receive? }
          end
        else
          upstream.close rescue nil
        end
      end
    end

    # Manages permanent AMQP, MQTT, and HTTP listener sockets across role transitions.
    # - On the Leader: dispatches AMQP/MQTT connections directly to the local server.
    # - On a Follower: transparently Layer-4 splices (`IO.copy`) AMQP and MQTT connections
    #   to the current Leader without closing or rebinding listener sockets.
    class IngressRouter
      Log = LavinMQ::Log.for "clustering_v2.ingress"

      private struct ActiveSplice
        getter client_sock : IO
        getter upstream_sock : IO

        def initialize(@client_sock : IO, @upstream_sock : IO)
        end

        def close : Nil
          @client_sock.close rescue nil
          @upstream_sock.close rescue nil
        end
      end

      getter node : RaftNode
      getter execution_context : Fiber::ExecutionContext
      getter server : LavinMQ::Server
      getter amqp_server : LavinMQ::AMQP::Server
      getter mqtt_server : LavinMQ::MQTT::Server
      getter http_server : LavinMQ::HTTP::Server
      getter http_router : FollowerHttpRouterHandler
      getter amqp_port : Int32 = 0
      getter mqtt_port : Int32 = 0
      getter http_port : Int32 = 0

      @amqp_listener : TCPServer?
      @amqps_listener : TCPServer?
      @amqps_tls_context : OpenSSL::SSL::Context::Server?
      @amqp_unix_listener : UNIXServer?
      @mqtt_listener : TCPServer?
      @mqtts_listener : TCPServer?
      @mqtts_tls_context : OpenSSL::SSL::Context::Server?
      @mqtt_unix_listener : UNIXServer?
      @http_addr : Socket::IPAddress?
      @http_bound = false
      @splices = Array(ActiveSplice).new
      @splice_lock = Mutex.new
      @closed = false

      def initialize(@node : RaftNode,
                     @replicator : LogicalReplicator,
                     @server : LavinMQ::Server,
                     @config : Config = Config.instance,
                     execution_context : Fiber::ExecutionContext? = nil)
        @execution_context = execution_context || Fiber::ExecutionContext.default
        @amqp_server = LavinMQ::AMQP::Server.new(@server, @config)
        @amqp_server.leader_check = -> { ensure_promoted_leader? }
        @mqtt_server = LavinMQ::MQTT::Server.new(@server, @config)
        @mqtt_server.leader_check = -> { ensure_promoted_leader? }
        @http_router = FollowerHttpRouterHandler.new(@node)
        @http_server = LavinMQ::HTTP::Server.new(@server, @amqp_server, @mqtt_server, @http_router)

        @replicator.on_role_change do |new_role|
          if new_role.leader?
            close_active_splices
          end
        end
      end

      private def advertised_address(bind_addr : String) : String
        if bind_addr.in?("0.0.0.0", "::", "[::]")
          if !@node.peer_uri.empty? && (peer_host = (URI.parse(@node.peer_uri).host rescue nil)) &&
             !peer_host.in?("", "0.0.0.0", "::", "[::]")
            return peer_host
          end
          return "127.0.0.1"
        end
        bind_addr
      end

      def bind_amqp(host : String = "127.0.0.1", port : Int32 = 0) : Socket::IPAddress
        listener = TCPServer.new(host, port)
        @amqp_listener = listener
        addr = listener.local_address
        @amqp_port = addr.port
        @node.amqp_uri = "amqp://#{advertised_address(addr.address)}:#{addr.port}"
        addr
      end

      def bind_amqps(host : String, port : Int32, ctx : OpenSSL::SSL::Context::Server) : Socket::IPAddress
        listener = TCPServer.new(host, port)
        @amqps_listener = listener
        @amqps_tls_context = ctx
        addr = listener.local_address
        @node.amqp_uri = "amqps://#{advertised_address(addr.address)}:#{addr.port}" if @node.amqp_uri.empty?
        addr
      end

      def bind_amqp_unix(path : String) : Socket::UNIXAddress
        File.delete?(path)
        listener = UNIXServer.new(path)
        File.chmod(path, 0o666)
        @amqp_unix_listener = listener
        listener.local_address
      end

      def bind_mqtt(host : String = "127.0.0.1", port : Int32 = 0) : Socket::IPAddress
        listener = TCPServer.new(host, port)
        @mqtt_listener = listener
        addr = listener.local_address
        @mqtt_port = addr.port
        @node.mqtt_uri = "mqtt://#{advertised_address(addr.address)}:#{addr.port}"
        addr
      end

      def bind_mqtts(host : String, port : Int32, ctx : OpenSSL::SSL::Context::Server) : Socket::IPAddress
        listener = TCPServer.new(host, port)
        @mqtts_listener = listener
        @mqtts_tls_context = ctx
        addr = listener.local_address
        @node.mqtt_uri = "mqtts://#{advertised_address(addr.address)}:#{addr.port}" if @node.mqtt_uri.empty?
        addr
      end

      def bind_mqtt_unix(path : String) : Socket::UNIXAddress
        File.delete?(path)
        listener = UNIXServer.new(path)
        File.chmod(path, 0o666)
        @mqtt_unix_listener = listener
        listener.local_address
      end

      def bind_http(host : String = "127.0.0.1", port : Int32 = 0) : Socket::IPAddress
        addr = @http_server.bind_tcp(host, port)
        @http_addr = addr
        @http_bound = true
        @http_port = addr.port
        @node.http_uri = "http://#{advertised_address(addr.address)}:#{addr.port}"
        addr
      end

      def bind_https(host : String, port : Int32, ctx : OpenSSL::SSL::Context::Server) : Socket::IPAddress
        addr = @http_server.bind_tls(host, port, ctx)
        @http_bound = true
        @node.http_uri = "https://#{advertised_address(addr.address)}:#{addr.port}" if @node.http_uri.empty?
        addr
      end

      def bind_http_unix(path : String)
        addr = @http_server.bind_unix(path)
        @http_bound = true
        addr
      end

      def bind_http_internal_unix
        addr = @http_server.bind_internal_unix
        @http_bound = true
        addr
      end

      def start : Nil
        if amqp_l = @amqp_listener
          @execution_context.spawn(name: "ClusteringV2 AMQP permanent listener #{@node.node_id}") do
            accept_amqp_loop(amqp_l)
          end
        end
        if (amqps_l = @amqps_listener) && (amqps_ctx = @amqps_tls_context)
          @execution_context.spawn(name: "ClusteringV2 AMQPS permanent listener #{@node.node_id}") do
            accept_amqps_loop(amqps_l, amqps_ctx)
          end
        end
        if amqp_u = @amqp_unix_listener
          @execution_context.spawn(name: "ClusteringV2 AMQP UNIX permanent listener #{@node.node_id}") do
            accept_amqp_unix_loop(amqp_u)
          end
        end
        if mqtt_l = @mqtt_listener
          @execution_context.spawn(name: "ClusteringV2 MQTT permanent listener #{@node.node_id}") do
            accept_mqtt_loop(mqtt_l)
          end
        end
        if (mqtts_l = @mqtts_listener) && (mqtts_ctx = @mqtts_tls_context)
          @execution_context.spawn(name: "ClusteringV2 MQTTS permanent listener #{@node.node_id}") do
            accept_mqtts_loop(mqtts_l, mqtts_ctx)
          end
        end
        if mqtt_u = @mqtt_unix_listener
          @execution_context.spawn(name: "ClusteringV2 MQTT UNIX permanent listener #{@node.node_id}") do
            accept_mqtt_unix_loop(mqtt_u)
          end
        end
        if @http_bound
          @execution_context.spawn(name: "ClusteringV2 HTTP permanent listener #{@node.node_id}") do
            @http_server.listen
          rescue ex
            Log.debug { "HTTP listener stopped: #{ex.message}" } unless @closed
          end
        end
      end

      def active_splice_count : Int32
        @splice_lock.synchronize { @splices.size }
      end

      def close : Nil
        return if @closed
        @closed = true
        @amqp_listener.try &.close rescue nil
        @amqps_listener.try &.close rescue nil
        @amqp_unix_listener.try &.close rescue nil
        @mqtt_listener.try &.close rescue nil
        @mqtts_listener.try &.close rescue nil
        @mqtt_unix_listener.try &.close rescue nil
        close_active_splices
        @http_server.close rescue nil
        @amqp_server.close rescue nil
        @mqtt_server.close rescue nil
      end

      private def accept_amqp_loop(listener : TCPServer) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          client.tcp_nodelay = true
          @execution_context.spawn(name: "ClusteringV2 AMQP connection handler") do
            handle_amqp_socket(client)
          end
        end
      end

      private def accept_amqps_loop(listener : TCPServer, ctx : OpenSSL::SSL::Context::Server) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          client.tcp_nodelay = true
          @execution_context.spawn(name: "ClusteringV2 AMQPS connection handler") do
            handle_amqps_socket(client, ctx)
          end
        end
      end

      private def accept_amqp_unix_loop(listener : UNIXServer) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          @execution_context.spawn(name: "ClusteringV2 AMQP UNIX connection handler") do
            handle_amqp_unix_socket(client)
          end
        end
      end

      private def accept_mqtt_loop(listener : TCPServer) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          client.tcp_nodelay = true
          @execution_context.spawn(name: "ClusteringV2 MQTT connection handler") do
            handle_mqtt_socket(client)
          end
        end
      end

      private def accept_mqtts_loop(listener : TCPServer, ctx : OpenSSL::SSL::Context::Server) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          client.tcp_nodelay = true
          @execution_context.spawn(name: "ClusteringV2 MQTTS connection handler") do
            handle_mqtts_socket(client, ctx)
          end
        end
      end

      private def accept_mqtt_unix_loop(listener : UNIXServer) : Nil
        loop do
          break if @closed
          client = listener.accept? || break
          if @closed
            client.close rescue nil
            break
          end
          @execution_context.spawn(name: "ClusteringV2 MQTT UNIX connection handler") do
            handle_mqtt_unix_socket(client)
          end
        end
      end

      SPLICE_MAGIC = "LMQSPL01".to_slice

      private def ensure_promoted_leader? : Bool
        return true if @node.promoted?
        return false unless @node.leader?
        @node.wait_until_promoted(2.seconds)
      end

      private def compute_splice_hmac(ts : Int64, remote_ip : String, remote_port : UInt16, local_ip : String, local_port : UInt16, proxied : Bool) : Bytes
        io = IO::Memory.new(64)
        io.write(SPLICE_MAGIC)
        IO::ByteFormat::LittleEndian.encode(ts, io)
        Protocol.write_string(io, remote_ip)
        IO::ByteFormat::LittleEndian.encode(remote_port, io)
        Protocol.write_string(io, local_ip)
        IO::ByteFormat::LittleEndian.encode(local_port, io)
        io.write_byte(proxied ? 1_u8 : 0_u8)
        OpenSSL::HMAC.digest(:sha256, @node.secret, io.to_slice)
      end

      private def write_splice_header(upstream : IO, conn_info : ConnectionInfo) : Nil
        ts = Time.utc.to_unix_ms
        remote_ip = conn_info.remote_address.address
        remote_port = conn_info.remote_address.port
        local_ip = conn_info.local_address.address
        local_port = conn_info.local_address.port
        proxied = conn_info.proxied?
        mac = compute_splice_hmac(ts, remote_ip, remote_port, local_ip, local_port, proxied)

        upstream.write(SPLICE_MAGIC)
        IO::ByteFormat::LittleEndian.encode(ts, upstream)
        Protocol.write_string(upstream, remote_ip)
        IO::ByteFormat::LittleEndian.encode(remote_port, upstream)
        Protocol.write_string(upstream, local_ip)
        IO::ByteFormat::LittleEndian.encode(local_port, upstream)
        upstream.write_byte(proxied ? 1_u8 : 0_u8)
        upstream.write(mac)
        upstream.flush
      end

      private def read_splice_ip(client : IO) : String
        len = IO::ByteFormat::LittleEndian.decode(UInt32, client)
        if len == 0_u32 || len > 256_u32
          raise IO::Error.new("Invalid cluster splice IP length")
        end
        String.new(len.to_i) do |buf|
          client.read_fully(Slice.new(buf, len.to_i))
          {len.to_i, 0}
        end
      end

      private def extract_connection_info(client : IO::Buffered, remote_addr : Socket::IPAddress, local_addr : Socket::IPAddress) : ConnectionInfo
        client.read_timeout = 15.seconds
        peeked = client.peek
        if !peeked.empty? && peeked[0] == SPLICE_MAGIC[0]
          magic = uninitialized UInt8[8]
          client.read_fully(magic.to_slice)
          unless magic.to_slice == SPLICE_MAGIC
            raise IO::Error.new("Invalid cluster splice magic")
          end
          ts = IO::ByteFormat::LittleEndian.decode(Int64, client)
          remote_ip = read_splice_ip(client)
          remote_port = IO::ByteFormat::LittleEndian.decode(UInt16, client)
          local_ip = read_splice_ip(client)
          local_port = IO::ByteFormat::LittleEndian.decode(UInt16, client)
          proxied_byte = client.read_byte || raise IO::EOFError.new
          proxied = proxied_byte != 0_u8
          mac = uninitialized UInt8[32]
          client.read_fully(mac.to_slice)

          if (Time.utc.to_unix_ms - ts).abs > 60_000_i64
            raise IO::Error.new("Expired cluster splice header")
          end
          expected_mac = compute_splice_hmac(ts, remote_ip, remote_port, local_ip, local_port, proxied)
          unless Crypto::Subtle.constant_time_compare(mac.to_slice, expected_mac)
            raise IO::Error.new("Cluster splice HMAC verification failed")
          end
          return ConnectionInfo.new(
            Socket::IPAddress.new(remote_ip, remote_port.to_i32),
            Socket::IPAddress.new(local_ip, local_port.to_i32),
            proxied: proxied
          )
        end
        if @config.tcp_proxy_protocol?
          if proxied_info = ProxyProtocol.parse(client)
            return proxied_info
          end
        end
        ConnectionInfo.new(remote_addr, local_addr)
      ensure
        client.read_timeout = nil
      end

      private def extract_follower_connection_info(client : IO::Buffered, remote_addr : Socket::IPAddress, local_addr : Socket::IPAddress) : ConnectionInfo
        if @config.tcp_proxy_protocol?
          if proxied_info = ProxyProtocol.parse(client)
            return proxied_info
          end
        end
        ConnectionInfo.new(remote_addr, local_addr)
      end

      private def handle_amqp_socket(client : TCPSocket) : Nil
        remote_addr = client.remote_address
        local_addr = client.local_address
        if ensure_promoted_leader?
          conn_info = extract_connection_info(client, remote_addr, local_addr)
          @amqp_server.handle_connection(client, conn_info)
        else
          conn_info = extract_follower_connection_info(client, remote_addr, local_addr)
          splice_to_leader_amqp(client, conn_info)
        end
      rescue
        client.close rescue nil
      end

      private def handle_amqps_socket(client : TCPSocket, ctx : OpenSSL::SSL::Context::Server) : Nil
        remote_addr = client.remote_address
        local_addr = client.local_address
        ssl_client = OpenSSL::SSL::Socket::Server.new(client, ctx, sync_close: true)
        if ensure_promoted_leader?
          conn_info = extract_connection_info(ssl_client, remote_addr, local_addr)
          conn_info.ssl = true
          conn_info.ssl_version = ssl_client.tls_version
          conn_info.ssl_cipher = ssl_client.cipher
          @amqp_server.handle_connection(ssl_client, conn_info)
        else
          conn_info = extract_follower_connection_info(ssl_client, remote_addr, local_addr)
          splice_to_leader_amqp(ssl_client, conn_info)
        end
      rescue
        client.close rescue nil
      end

      private def handle_amqp_unix_socket(client : UNIXSocket) : Nil
        if ensure_promoted_leader?
          @amqp_server.handle_connection(client, ConnectionInfo.local)
        else
          splice_to_leader_amqp(client, ConnectionInfo.local)
        end
      rescue
        client.close rescue nil
      end

      private def handle_mqtt_socket(client : TCPSocket) : Nil
        remote_addr = client.remote_address
        local_addr = client.local_address
        if ensure_promoted_leader?
          conn_info = extract_connection_info(client, remote_addr, local_addr)
          @mqtt_server.handle_connection(client, conn_info)
        else
          conn_info = extract_follower_connection_info(client, remote_addr, local_addr)
          splice_to_leader_mqtt(client, conn_info)
        end
      rescue
        client.close rescue nil
      end

      private def handle_mqtts_socket(client : TCPSocket, ctx : OpenSSL::SSL::Context::Server) : Nil
        remote_addr = client.remote_address
        local_addr = client.local_address
        ssl_client = OpenSSL::SSL::Socket::Server.new(client, ctx, sync_close: true)
        if ensure_promoted_leader?
          conn_info = extract_connection_info(ssl_client, remote_addr, local_addr)
          conn_info.ssl = true
          conn_info.ssl_version = ssl_client.tls_version
          conn_info.ssl_cipher = ssl_client.cipher
          @mqtt_server.handle_connection(ssl_client, conn_info)
        else
          conn_info = extract_follower_connection_info(ssl_client, remote_addr, local_addr)
          splice_to_leader_mqtt(ssl_client, conn_info)
        end
      rescue
        client.close rescue nil
      end

      private def handle_mqtt_unix_socket(client : UNIXSocket) : Nil
        if ensure_promoted_leader?
          @mqtt_server.handle_connection(client, ConnectionInfo.local)
        else
          splice_to_leader_mqtt(client, ConnectionInfo.local)
        end
      rescue
        client.close rescue nil
      end

      private def splice_to_leader_amqp(client : IO, conn_info : ConnectionInfo) : Nil
        leader = @node.leader_member
        if leader.nil? || leader.node_id == @node.node_id || leader.amqp_uri.empty?
          client.close rescue nil
          return
        end
        splice_to_upstream(client, leader.amqp_uri, "amqps", 5672, 5671, conn_info)
      end

      private def splice_to_leader_mqtt(client : IO, conn_info : ConnectionInfo) : Nil
        leader = @node.leader_member
        if leader.nil? || leader.node_id == @node.node_id || leader.mqtt_uri.empty?
          client.close rescue nil
          return
        end
        splice_to_upstream(client, leader.mqtt_uri, "mqtts", 1883, 8883, conn_info)
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def splice_to_upstream(client : IO,
                                     target_uri : String,
                                     tls_scheme : String,
                                     default_port : Int32,
                                     default_tls_port : Int32,
                                     conn_info : ConnectionInfo) : Nil
        uri = URI.parse(target_uri)
        host = uri.host || "127.0.0.1"
        port = uri.port || (uri.scheme == tls_scheme ? default_tls_port : default_port)

        tcp_sock = TCPSocket.new(host, port, connect_timeout: 2.seconds)
        tcp_sock.tcp_nodelay = true
        tcp_sock.sync = true
        tcp_sock.read_buffering = false
        upstream : IO = if uri.scheme == tls_scheme
          tls_ctx = OpenSSL::SSL::Context::Client.new
          tls_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
          ssl_sock = OpenSSL::SSL::Socket::Client.new(tcp_sock, context: tls_ctx, sync_close: true, hostname: host)
          ssl_sock.sync = true
          ssl_sock
        else
          tcp_sock
        end
        write_splice_header(upstream, conn_info)
        client.sync = true if client.responds_to?(:sync=)
        client.read_buffering = false if client.responds_to?(:read_buffering=)

        splice = ActiveSplice.new(client, upstream)
        @splice_lock.synchronize { @splices << splice }

        done = ::Channel(Nil).new(2)
        @execution_context.spawn(name: "ClusteringV2 splice upstream->client") do
          IO.copy(upstream, client)
        rescue IO::Error
        ensure
          splice.close
          done.send(nil) rescue nil
        end

        begin
          IO.copy(client, upstream)
        rescue IO::Error
        ensure
          splice.close
          done.send(nil) rescue nil
        end

        2.times { done.receive? }
        @splice_lock.synchronize { @splices.delete(splice) }
      rescue
        client.close rescue nil
      end

      private def close_active_splices : Nil
        to_close = @splice_lock.synchronize do
          dup = @splices.dup
          @splices.clear
          dup
        end
        to_close.each(&.close)
      end
    end

    # Self-contained Hot-Standby Cluster Node combining RaftLog, RaftNode,
    # LogicalReplicator, LavinMQ::Server, and IngressRouter.
    class ClusterNode
      getter config : Config
      getter data_dir : String
      getter execution_context : Fiber::ExecutionContext
      getter log : RaftLog
      getter raft : RaftNode
      getter replicator : LogicalReplicator
      getter server : LavinMQ::Server
      getter router : IngressRouter
      @had_persisted_members : Bool

      def self.from_config(config : Config) : self
        role = config.clustering_role.downcase == "learner" ? MemberRole::Learner : MemberRole::Voter
        new(
          config.data_dir,
          node_id: config.clustering_node_id,
          secret: config.clustering_secret,
          initial_role: role,
          config: config
        )
      end

      def self.normalize_peer_uri(raw : String?) : String
        return "" unless str = raw
        trimmed = str.strip
        return "" if trimmed.empty?
        trimmed.includes?("://") ? trimmed : "tcp://#{trimmed}"
      end

      def self.parse_peer_spec(spec : String) : MemberInfo
        role = MemberRole::Voter
        main_part = spec.strip
        if hash_idx = main_part.rindex('#')
          role_str = main_part[(hash_idx + 1)..].downcase
          role = role_str == "learner" ? MemberRole::Learner : MemberRole::Voter
          main_part = main_part[0...hash_idx]
        end
        at_idx = main_part.index('@') || raise ArgumentError.new("Invalid peer spec '#{spec}': missing '@'")
        node_id = main_part[0...at_idx].to_u64
        uris = main_part[(at_idx + 1)..].split('|')
        peer_uri = normalize_peer_uri(uris[0])
        amqp_uri = uris[1]? || ""
        http_uri = uris[2]? || ""
        mqtt_uri = uris[3]? || ""
        MemberInfo.new(node_id, peer_uri, amqp_uri, http_uri, role, mqtt_uri)
      end

      def initialize(@data_dir : String,
                     node_id : UInt64,
                     secret : String,
                     initial_role : MemberRole = MemberRole::Voter,
                     commit_gated_delivery : Bool = true,
                     ring_capacity : UInt64 = 4096_u64,
                     execution_context : Fiber::ExecutionContext? = nil,
                     ingress_execution_context : Fiber::ExecutionContext? = nil,
                     config : Config? = nil)
        raise ArgumentError.new("Cluster secret must not be empty") if secret.blank?
        FileUtils.mkdir_p(@data_dir)
        @execution_context = execution_context || Fiber::ExecutionContext::Parallel.new("ClusteringV2-Node-#{node_id}", 4)
        @config = config || Config.instance.dup.tap do |cfg|
          cfg.data_dir = @data_dir
        end
        @log = RaftLog.new(@data_dir, capacity: ring_capacity, node_id: node_id)
        @had_persisted_members = @log.meta_existed? && (!@log.committed_members.empty? || !@log.members.empty?)
        @raft = RaftNode.new(@log, secret: secret, initial_role: initial_role, execution_context: @execution_context)
        @replicator = LogicalReplicator.new(@data_dir, @raft, commit_gated_delivery: commit_gated_delivery)
        @server = LavinMQ::Server.new(@config, @replicator)
        @replicator.attach(@server)
        @router = IngressRouter.new(@raft, @replicator, @server, @config, ingress_execution_context)
      end

      def bind(host : String = "127.0.0.1",
               peer_port : Int32 = 0,
               amqp_port : Int32 = 0,
               http_port : Int32 = 0,
               mqtt_port : Int32? = nil) : MemberInfo
        @router.bind_amqp(host, amqp_port)
        if mp = mqtt_port
          @router.bind_mqtt(host, mp)
        end
        @router.bind_http(host, http_port)
        @raft.start(host, peer_port)
        @router.start
        MemberInfo.new(
          @raft.node_id,
          @raft.peer_uri,
          @raft.amqp_uri,
          @raft.http_uri,
          @raft.role.learner? ? MemberRole::Learner : MemberRole::Voter,
          @raft.mqtt_uri
        )
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def start_from_config(config : Config = @config,
                            amqp_tls_ctx : OpenSSL::SSL::Context::Server? = nil,
                            mqtt_tls_ctx : OpenSSL::SSL::Context::Server? = nil,
                            http_tls_ctx : OpenSSL::SSL::Context::Server? = nil) : MemberInfo
        adv_peer = ClusterNode.normalize_peer_uri(config.clustering_advertised_uri)
        @raft.peer_uri = adv_peer unless adv_peer.empty?

        self_spec_info : MemberInfo? = nil
        config.clustering_peers.each do |peer_spec|
          next if peer_spec.empty?
          info = ClusterNode.parse_peer_spec(peer_spec)
          if info.node_id == @raft.node_id
            self_spec_info = info
            @raft.peer_uri = info.peer_uri if @raft.peer_uri.empty? && !info.peer_uri.empty?
          end
        end

        @router.bind_amqp(config.amqp_bind, config.amqp_port) if config.amqp_port > 0
        if config.amqps_port > 0 && (a_ctx = amqp_tls_ctx)
          @router.bind_amqps(config.amqp_bind, config.amqps_port, a_ctx)
        end
        @router.bind_amqp_unix(config.unix_path) unless config.unix_path.empty?

        @router.bind_mqtt(config.mqtt_bind, config.mqtt_port) if config.mqtt_port > 0
        if config.mqtts_port > 0 && (m_ctx = mqtt_tls_ctx)
          @router.bind_mqtts(config.mqtt_bind, config.mqtts_port, m_ctx)
        end
        @router.bind_mqtt_unix(config.mqtt_unix_path) unless config.mqtt_unix_path.empty?

        @router.bind_http(config.http_bind, config.http_port) if config.http_port > 0
        if config.https_port > 0 && (h_ctx = http_tls_ctx)
          @router.bind_https(config.http_bind, config.https_port, h_ctx)
        end
        @router.bind_http_unix(config.http_unix_path) unless config.http_unix_path.empty?
        @router.bind_http_internal_unix

        if s_info = self_spec_info
          @raft.amqp_uri = s_info.amqp_uri unless s_info.amqp_uri.empty?
          @raft.http_uri = s_info.http_uri unless s_info.http_uri.empty?
          @raft.mqtt_uri = s_info.mqtt_uri unless s_info.mqtt_uri.empty?
        end

        @raft.start(config.clustering_bind, config.clustering_port)
        @router.start
        self_info = MemberInfo.new(
          @raft.node_id,
          @raft.peer_uri,
          @raft.amqp_uri,
          @raft.http_uri,
          @raft.role.learner? ? MemberRole::Learner : MemberRole::Voter,
          @raft.mqtt_uri
        )
        if @had_persisted_members
          @log.update_member_uris(@raft.node_id, @raft.peer_uri, @raft.amqp_uri, @raft.http_uri, @raft.mqtt_uri)
        else
          members_by_id = {self_info.node_id => self_info}
          config.clustering_peers.each do |peer_spec|
            next if peer_spec.empty?
            info = ClusterNode.parse_peer_spec(peer_spec)
            members_by_id[info.node_id] = info unless info.node_id == self_info.node_id
          end
          @raft.configure_cluster(members_by_id.values)
        end
        self_info
      end

      def close : Nil
        @router.close
        @server.close
        @replicator.close
      end
    end
  end
end
