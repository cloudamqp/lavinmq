require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module LavinMQ
  module HTTP
    Log = LavinMQ::Log.for "http"

    class Server
      Log = LavinMQ::Log.for "http.server"
      @tls_context : OpenSSL::SSL::Context::Server?
      @config : LavinMQ::Config

      def initialize(@amqp_server : LavinMQ::Server)
        @config = @amqp_server.@config
        create_tls_context if @config.tls_configured?
        handlers = [
          StrictTransportSecurity.new,
          WebsocketProxy.new(@amqp_server),
          StaticController.new,
          ViewsController.new,
          ApiErrorHandler.new,
          AuthHandler.new(@amqp_server),
          PrometheusController.new(@amqp_server),
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
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(log: Log)) if Log.level == ::Log::Severity::Debug
        @http = ::HTTP::Server.new(handlers)
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        Log.info { "Bound to #{addr}" }
        addr
      end

      def bind_tls(address, port)
        raise "Missing @tls_context" unless ctx = @tls_context
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
        File.delete?(INTERNAL_UNIX_SOCKET)
        addr = @http.bind_unix(INTERNAL_UNIX_SOCKET)
        File.chmod(INTERNAL_UNIX_SOCKET, 0o660)
        Log.info { "Bound to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
        File.delete?(INTERNAL_UNIX_SOCKET)
      end

      # Starts a HTTP server that binds to the internal UNIX socket used by lavinmqctl.
      # The server returns 503 to signal that the node is a follower and can not handle the request.
      def self.follower_internal_socket_http_server
        http_server = ::HTTP::Server.new do |context|
          context.response.status_code = 503
          context.response.print "This node is a follower and does not handle lavinmqctl commands. \n" \
                                 "Please connect to the leader node by using the --host option."
        end

        File.delete?(INTERNAL_UNIX_SOCKET)
        addr = http_server.bind_unix(INTERNAL_UNIX_SOCKET)
        File.chmod(INTERNAL_UNIX_SOCKET, 0o660)
        Log.info { "Bound to #{addr}" }

        spawn(name: "HTTP listener") do
          http_server.listen
        end
      end

      private def create_tls_context
        context = OpenSSL::SSL::Context::Server.new
        context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
        configure_tls_version(context)
        context.certificate_chain = @config.tls_cert_path
        context.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
        context.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
        @tls_context = context
      end

      def reload_tls_context
        create_tls_context
      end

      private def configure_tls_version(tls : OpenSSL::SSL::Context::Server)
        case @config.tls_min_version
        when "1.0"
          tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                             OpenSSL::SSL::Options::NO_TLS_V1_1 |
                             OpenSSL::SSL::Options::NO_TLS_V1)
        when "1.1"
          tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
          tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
        when "1.2", ""
          tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
          tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
        when "1.3"
          tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        else
          Log.warn { "Unrecognized @config value for tls_min_version: '#{@config.tls_min_version}'" }
        end
      end
    end
  end
end
