require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/*"
require "../auth/user"

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
      @internal_unix_socket_path : String = Config.instance.control_unix_path

      def initialize(@amqp_server : LavinMQ::Server)
        oauth_authenticator =
          case auth = @amqp_server.authenticator
          when Auth::Chain
            auth.backends.select(Auth::OAuthAuthenticator).first?
          when Auth::OAuthAuthenticator
            auth
          end
        handlers = [
          (::HTTP::LogHandler.new(log: Log) if Log.level == ::Log::Severity::Debug),
          StrictTransportSecurity.new,
          WebsocketProxy.new(@amqp_server),
          ViewsController.new,
          StaticController.new,
          oauth_authenticator && OAuthController.new(oauth_authenticator),
          AuthHandler.new(@amqp_server.authenticator, @amqp_server.users.direct_user, @internal_unix_socket_path),
          ApiErrorHandler.new,
          RequireUserHandler.new,
          PrometheusController.new(@amqp_server, require_authentication: true),
          ApiDefaultsHandler.new,
          MainController.new(@amqp_server),
          DefinitionsController.new(@amqp_server),
          ConnectionsController.new(@amqp_server),
          ChannelsController.new(@amqp_server),
          ConsumersController.new(@amqp_server),
          ExchangesController.new(@amqp_server),
          QueuesController.new(@amqp_server),
          BindingsController.new(@amqp_server),
          VHostsController.new(@amqp_server),
          VHostLimitsController.new(@amqp_server),
          UsersController.new(@amqp_server),
          PermissionsController.new(@amqp_server),
          ParametersController.new(@amqp_server),
          ShovelsController.new(@amqp_server),
          NodesController.new(@amqp_server),
          LogsController.new(@amqp_server),
        ].select(::HTTP::Handler) # drops nil entries and types the array to Array(::HTTP::Handler)
        @http = ::HTTP::Server.new(handlers)
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
        File.chmod(@internal_unix_socket_path, 0o660)
        Log.info { "Bound to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
        File.delete?(@internal_unix_socket_path)
      end

      # Starts a HTTP server that binds to the internal UNIX socket used by lavinmqctl.
      # The server returns 503 to signal that the node is a follower and can not handle the request.
      # If another node on the same machine already serves the socket the server is
      # skipped and nil is returned, it's only a convenience for lavinmqctl users.
      def self.follower_internal_socket_http_server : ::HTTP::Server?
        path = Config.instance.control_unix_path
        http_server = ::HTTP::Server.new do |context|
          context.response.status_code = 503
          context.response.print "This node is a follower and does not handle lavinmqctl commands. \n" \
                                 "Please connect to the leader node by using the --host option."
        end

        begin
          prepare_control_socket(path)
          addr = http_server.bind_unix(path)
        rescue ex : Socket::BindError
          Log.warn { "#{ex.message}, not serving lavinmqctl socket on this node" }
          http_server.close
          return
        rescue ex
          Log.warn { "#{ex.message}, not serving lavinmqctl socket on this node" }
          http_server.close
          return
        end

        File.chmod(path, 0o660)
        Log.info { "Bound to #{addr}" }

        spawn(name: "HTTP listener") do
          http_server.listen
        rescue ex
          raise ex unless http_server.closed? # closed before listen started
        end
        http_server
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
  end
end
