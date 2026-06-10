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

    class Server
      Log = LavinMQ::Log.for "http.server"

      # Resolved once and reused for this server's lifetime so a later config
      # reload (SIGHUP) can't make us delete or authenticate against a path
      # different from the one we actually bound.
      @internal_unix_socket_path : String = Config.instance.control_unix_path

      def initialize(@server : LavinMQ::Server, @amqp_server : LavinMQ::AMQP::Server, @mqtt_server : LavinMQ::MQTT::Server)
        handlers = [
          StrictTransportSecurity.new,
          WebsocketProxy.new(@amqp_server, @mqtt_server),
          ViewsController.new,
          StaticController.new,
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
          ParametersController.new(@server),
          ShovelsController.new(@server),
          NodesController.new(@server),
          LogsController.new(@server),
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(log: Log)) if Log.level == ::Log::Severity::Debug
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
        File.delete?(@internal_unix_socket_path)
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
      def self.follower_internal_socket_http_server
        path = Config.instance.control_unix_path
        http_server = ::HTTP::Server.new do |context|
          context.response.status_code = 503
          context.response.print "This node is a follower and does not handle lavinmqctl commands. \n" \
                                 "Please connect to the leader node by using the --host option."
        end

        File.delete?(path)
        addr = http_server.bind_unix(path)
        File.chmod(path, 0o660)
        Log.info { "Bound to #{addr}" }

        spawn(name: "HTTP listener") do
          http_server.listen
        end
      end
    end
  end
end
