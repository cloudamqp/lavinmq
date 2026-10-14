require "http/client"
require "http/server"
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
        if @node.leader?
          context.response.headers["X-LavinMQ-Cluster-Role"] = "leader"
          context.response.headers["X-LavinMQ-Node-Id"] = @node.node_id.to_s
          call_next(context)
          return
        end

        method = context.request.method
        path = context.request.path

        if FollowerHttpRouterHandler.serve_locally_on_follower?(method, path)
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

      def self.serve_locally_on_follower?(method : String, path : String) : Bool
        return false unless method == "GET" || method == "HEAD"

        # Connection, channel, and consumer listings reflect live TCP sockets on the leader
        return false if path == "/api/connections" || path.starts_with?("/api/connections/")
        return false if path.ends_with?("/connections")
        return false if path == "/api/channels" || path.starts_with?("/api/channels/")
        return false if path == "/api/consumers" || path.starts_with?("/api/consumers/")

        true
      end

      private def proxy_to_leader(context : ::HTTP::Server::Context) : Nil
        leader = @node.leader_member
        if leader.nil? || leader.http_uri.empty?
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print(%({"error":"Cluster leader currently unavailable"}))
          return
        end

        uri = URI.parse(leader.http_uri)
        host = uri.host || "127.0.0.1"
        port = uri.port || 15672

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
    end

    # Manages permanent AMQP, MQTT, and HTTP listener sockets across role transitions.
    # - On the Leader: dispatches AMQP/MQTT connections directly to the local server.
    # - On a Follower: transparently Layer-4 splices (`IO.copy`) AMQP/MQTT connections
    #   to the current Leader without closing or rebinding listener sockets.
    class IngressRouter
      Log = LavinMQ::Log.for "clustering_v2.ingress"

      private struct ActiveSplice
        getter client_sock : TCPSocket
        getter upstream_sock : TCPSocket

        def initialize(@client_sock : TCPSocket, @upstream_sock : TCPSocket)
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
      getter http_port : Int32 = 0

      @amqp_listener : TCPServer?
      @http_addr : Socket::IPAddress?
      @splices = Array(ActiveSplice).new
      @splice_lock = Mutex.new
      @closed = false

      def initialize(@node : RaftNode,
                     @replicator : LogicalReplicator,
                     @server : LavinMQ::Server,
                     config : Config = Config.instance,
                     execution_context : Fiber::ExecutionContext? = nil)
        @execution_context = execution_context || Fiber::ExecutionContext.default
        @amqp_server = LavinMQ::AMQP::Server.new(@server, config)
        @mqtt_server = LavinMQ::MQTT::Server.new(@server, config)
        @http_router = FollowerHttpRouterHandler.new(@node)
        @http_server = LavinMQ::HTTP::Server.new(@server, @amqp_server, @mqtt_server, @http_router)

        @replicator.on_role_change do |new_role|
          if new_role.leader?
            close_active_splices
          end
        end
      end

      def bind_amqp(host : String = "127.0.0.1", port : Int32 = 0) : Socket::IPAddress
        listener = TCPServer.new(host, port)
        @amqp_listener = listener
        addr = listener.local_address
        @amqp_port = addr.port
        @node.amqp_uri = "amqp://#{addr.address}:#{addr.port}"
        addr
      end

      def bind_http(host : String = "127.0.0.1", port : Int32 = 0) : Socket::IPAddress
        addr = @http_server.bind_tcp(host, port)
        @http_addr = addr
        @http_port = addr.port
        @node.http_uri = "http://#{addr.address}:#{addr.port}"
        addr
      end

      def start : Nil
        if amqp_l = @amqp_listener
          @execution_context.spawn(name: "ClusteringV2 AMQP permanent listener #{@node.node_id}") do
            accept_amqp_loop(amqp_l)
          end
        end
        if @http_addr
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

      private def handle_amqp_socket(client : TCPSocket) : Nil
        if @node.leader?
          conn_info = ConnectionInfo.new(client.remote_address, client.local_address)
          @amqp_server.handle_connection(client, conn_info)
        else
          splice_to_leader_amqp(client)
        end
      rescue
        client.close rescue nil
      end

      private def splice_to_leader_amqp(client : TCPSocket) : Nil
        leader = @node.leader_member
        if leader.nil? || leader.amqp_uri.empty?
          client.close rescue nil
          return
        end

        uri = URI.parse(leader.amqp_uri)
        host = uri.host || "127.0.0.1"
        port = uri.port || 5672

        upstream = TCPSocket.new(host, port, connect_timeout: 2.seconds)
        upstream.tcp_nodelay = true
        upstream.sync = true
        upstream.read_buffering = false
        client.sync = true
        client.read_buffering = false

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
        raw_peer = uris[0]
        peer_uri = raw_peer.includes?("://") ? raw_peer : "tcp://#{raw_peer}"
        amqp_uri = uris[1]? || ""
        http_uri = uris[2]? || ""
        MemberInfo.new(node_id, peer_uri, amqp_uri, http_uri, role)
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
        @config = config || Config.new.tap do |cfg|
          cfg.data_dir = @data_dir
        end
        @log = RaftLog.new(@data_dir, capacity: ring_capacity, node_id: node_id)
        @raft = RaftNode.new(@log, secret: secret, initial_role: initial_role, execution_context: @execution_context)
        @replicator = LogicalReplicator.new(@data_dir, @raft, commit_gated_delivery: commit_gated_delivery)
        @server = LavinMQ::Server.new(@config, @replicator)
        @replicator.attach(@server)
        @router = IngressRouter.new(@raft, @replicator, @server, @config, ingress_execution_context)
      end

      def bind(host : String = "127.0.0.1",
               peer_port : Int32 = 0,
               amqp_port : Int32 = 0,
               http_port : Int32 = 0) : MemberInfo
        @router.bind_amqp(host, amqp_port)
        @router.bind_http(host, http_port)
        @raft.start(host, peer_port)
        @router.start
        MemberInfo.new(
          @raft.node_id,
          @raft.peer_uri,
          @raft.amqp_uri,
          @raft.http_uri,
          @raft.role.learner? ? MemberRole::Learner : MemberRole::Voter
        )
      end

      def start_from_config(config : Config = @config) : MemberInfo
        @router.bind_amqp(config.amqp_bind, config.amqp_port) if config.amqp_port > 0
        @router.bind_http(config.http_bind, config.http_port) if config.http_port > 0
        @router.http_server.bind_internal_unix
        @raft.start(config.clustering_bind, config.clustering_port)
        @router.start
        self_info = MemberInfo.new(
          @raft.node_id,
          @raft.peer_uri,
          @raft.amqp_uri,
          @raft.http_uri,
          @raft.role.learner? ? MemberRole::Learner : MemberRole::Voter
        )
        members_by_id = {self_info.node_id => self_info}
        config.clustering_peers.each do |peer_spec|
          next if peer_spec.empty?
          info = ClusterNode.parse_peer_spec(peer_spec)
          members_by_id[info.node_id] = info unless info.node_id == self_info.node_id
        end
        @raft.configure_cluster(members_by_id.values)
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
