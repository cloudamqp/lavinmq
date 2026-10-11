require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/*"
require "../amqp/server"
require "../auth/user"
require "../mqtt/server"

class HTTP::Server::Context
  property user : LavinMQ::Auth::BaseUser? = nil
end

module LavinMQ
  module HTTP
    Log = LavinMQ::Log.for "http"

    class ControlSocketInUseError < Exception; end

    class Server
      Log = LavinMQ::Log.for "http.server"

      # Resolved once and reused for this server's lifetime so a later config
      # reload (SIGHUP) can't make us delete or authenticate against a path
      # different from the one we actually bound.
      @internal_unix_socket_path : String
      @internal_bound = false
      # The whole API, also served over a raft node's ControlSocket
      getter handler : ::HTTP::Handler

      def initialize(@server : LavinMQ::Server, @amqp_server : LavinMQ::AMQP::Server, @mqtt_server : LavinMQ::MQTT::Server,
                     cluster : Clustering::RaftController? = nil,
                     @internal_unix_socket_path = Config.instance.control_unix_path)
        oauth_authenticator =
          case auth = @server.authenticator
          when Auth::Chain
            auth.backends.select(Auth::OAuthAuthenticator).first?
          when Auth::OAuthAuthenticator
            auth
          end
        handlers = [
          (::HTTP::LogHandler.new(log: Log) if Log.level == ::Log::Severity::Debug),
          StrictTransportSecurity.new,
          WebsocketProxy.new(@amqp_server, @mqtt_server),
          ViewsController.new,
          StaticController.new,
          oauth_authenticator && OAuthController.new(oauth_authenticator),
          AuthHandler.new(@server.authenticator, @server.users.direct_user, @internal_unix_socket_path),
          ApiErrorHandler.new,
          RequireUserHandler.new,
          PrometheusController.new(@server, require_authentication: true, raft: cluster.try(&.node)),
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
          ClusterController.new(@server, cluster),
          LogsController.new(@server),
        ].select(::HTTP::Handler) # drops nil entries and types the array to Array(::HTTP::Handler)
        @handler = ::HTTP::Server.build_middleware(handlers)
        @http = ::HTTP::Server.new(@handler)
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        Log.info { "Bound to #{addr}" }
        addr
      end

      def bind_tls(address, port, ctx)
        addr = @http.bind_tls address, port, ctx
        Log.info { "Bound on #{addr}" }
        addr
      end

      def bind_unix(path)
        File.delete?(path)
        addr = @http.bind_unix(path)
        File.chmod(path, 0o666)
        Log.info { "Bound to #{addr}" }
        addr
      end

      def bind_internal_unix
        Server.prepare_control_socket(@internal_unix_socket_path)
        addr = @http.bind_unix(@internal_unix_socket_path)
        @internal_bound = true
        File.chmod(@internal_unix_socket_path, 0o660)
        Log.info { "Bound to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def bound? : Bool
        !@http.addresses.empty?
      end

      def close
        @http.try &.close
        File.delete?(@internal_unix_socket_path) if @internal_bound
      end

      # A ControlSocket answering as a follower, see there. If another node on
      # the same machine already serves the socket it's skipped and nil is
      # returned, it's only a convenience for lavinmqctl users.
      def self.follower_internal_socket_http_server(cluster_status : Proc(String?)? = nil,
                                                    path = Config.instance.control_unix_path) : ControlSocket?
        socket = ControlSocket.new(path, cluster_status)
        socket if socket.bind
      end

      # Verifies that the control socket path is safe to bind to.
      # Deletes the file if it's a socket no one is listening on,
      # raises if it's in use, not a socket, or can't be verified.
      def self.prepare_control_socket(path)
        return unless info = File.info?(path, follow_symlinks: false)

        unless info.type.socket?
          raise "Control socket #{path} exists and is not a socket"
        end

        begin
          UNIXSocket.open(path) { }
          raise ControlSocketInUseError.new("Control socket #{path} is already in use")
        rescue Socket::ConnectError
          # ECONNREFUSED: socket inode exists, but nobody is listening.
          File.delete(path)
        rescue ex : Socket::Error
          # EACCES or anything ambiguous: fail closed, don't delete.
          raise "Cannot verify stale control socket #{path}: #{ex.message}"
        end
      end
    end

    # The internal UNIX socket used by lavinmqctl. A raft node binds it once
    # and keeps it through its role changes: while it serves as the leader,
    # requests go to the broker's HTTP API (#api=). Otherwise they get a 503
    # saying it's a follower, or with *cluster_status*, GET /api/cluster gets
    # this node's view of the cluster, so it can be inspected also when
    # there's no leader to proxy to.
    class ControlSocket
      Log = LavinMQ::Log.for "http.control_socket"

      getter path : String
      @api : ::HTTP::Handler? = nil
      @api_lock = Mutex.new
      @bound = false

      def initialize(@path : String, @cluster_status : Proc(String?)? = nil)
        @http = ::HTTP::Server.new { |context| handle(context) }
      end

      # Binds and listens. False, with a warning, if it can't, e.g. because
      # another node on this machine serves the socket.
      def bind : Bool
        return true if @bound
        Server.prepare_control_socket(@path)
        addr = @http.bind_unix(@path)
        @bound = true
        File.chmod(@path, 0o660)
        Log.info { "Bound to #{addr}" }
        spawn(name: "Control socket listener") do
          @http.listen
        rescue ex
          raise ex unless @http.closed? # closed before listen started
        end
        true
      rescue ex
        Log.warn { "#{ex.message}, not serving lavinmqctl socket on this node" }
        false
      end

      # Where requests go while this node serves, nil when it doesn't
      def api=(handler : ::HTTP::Handler?) : Nil
        @api_lock.synchronize { @api = handler }
      end

      def close : Nil
        @http.close
        File.delete?(@path) if @bound
        @bound = false
      end

      private def handle(context : ::HTTP::Server::Context) : Nil
        if api = @api_lock.synchronize { @api }
          return api.call(context)
        end
        if (status = @cluster_status) && context.request.method == "GET" &&
           context.request.path == "/api/cluster" && (json = status.call)
          context.response.content_type = "application/json"
          context.response.print json
          return
        end
        context.response.status_code = 503
        context.response.print "This node is a follower and does not handle lavinmqctl commands. \n" \
                               "Please connect to the leader node by using the --host option."
      end
    end
  end
end
