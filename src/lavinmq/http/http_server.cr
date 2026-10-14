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

      # The whole API, also served over the lavinmqctl socket, see ControlSocket
      getter handler : ::HTTP::Handler

      # Requests over *control_path*, the lavinmqctl socket, are authenticated
      # as the direct user. *metrics* writes the runner's own metrics, see
      # Runner#write_metrics.
      def initialize(@server : LavinMQ::Server, @amqp_server : LavinMQ::AMQP::Server, @mqtt_server : LavinMQ::MQTT::Server,
                     cluster : Clustering::RaftController? = nil,
                     control_path = Config.instance.control_unix_path,
                     metrics : Proc(PrometheusWriter, Nil)? = nil)
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
          AuthHandler.new(@server.authenticator, @server.users.direct_user, control_path),
          ApiErrorHandler.new,
          RequireUserHandler.new,
          PrometheusController.new(@server, require_authentication: true, metrics: metrics),
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
        @processor = ::HTTP::Server::RequestProcessor.new(@handler)
      end

      @closed = false

      # A connection accepted on a listener ClientListeners owns, over TLS
      # with *tls*
      def accept(client : Socket, tls : OpenSSL::SSL::Context::Server?) : Nil
        return client.close if @closed
        spawn(name: "HTTP client") do
          client.sync = false
          io = tls ? OpenSSL::SSL::Socket::Server.new(client, tls, sync_close: true) : client
          # process(io, io) wraps the IO from Crystal 1.22, losing the remote
          # address, e.g. for the loopback-only default user
          {% if compare_versions(Crystal::VERSION, "1.22.0") >= 0 %}
            @processor.process(io)
          {% else %}
            @processor.process(io, io)
          {% end %}
        rescue ex : IO::Error | OpenSSL::SSL::Error
          Log.debug { "HTTP connection error: #{ex.message}" }
        ensure
          io.try &.close rescue nil
          client.close rescue nil
        end
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

      def listen
        @http.listen
      end

      def close
        @closed = true
        # Ends keep-alive connections after their current request
        @processor.close
        @http.try &.close
      end
    end
  end
end
