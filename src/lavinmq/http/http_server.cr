require "http/server"
require "json"
require "router"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module LavinMQ
  module HTTP
    class Server
      def initialize(@amqp_server : LavinMQ::Server)
        @log = Log.for "http"
        handlers = [
          StrictTransportSecurity.new,
          AMQPWebsocket.new(@amqp_server),
          ViewsController.new.route_handler,
          StaticController.new,
          ApiErrorHandler.new(@log),
          AuthHandler.new(@amqp_server, @log),
          PrometheusController.new(@amqp_server, @log).route_handler,
          ApiDefaultsHandler.new,
          MainController.new(@amqp_server, @log).route_handler,
          DefinitionsController.new(@amqp_server, @log).route_handler,
          ConnectionsController.new(@amqp_server, @log).route_handler,
          ChannelsController.new(@amqp_server, @log).route_handler,
          ConsumersController.new(@amqp_server, @log).route_handler,
          ExchangesController.new(@amqp_server, @log).route_handler,
          QueuesController.new(@amqp_server, @log).route_handler,
          BindingsController.new(@amqp_server, @log).route_handler,
          VHostsController.new(@amqp_server, @log).route_handler,
          VHostLimitsController.new(@amqp_server, @log).route_handler,
          UsersController.new(@amqp_server, @log).route_handler,
          PermissionsController.new(@amqp_server, @log).route_handler,
          ParametersController.new(@amqp_server, @log).route_handler,
          NodesController.new(@amqp_server, @log).route_handler,
          LogsController.new(@amqp_server, @log).route_handler,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(@log)) if @log.level == Log::Severity::Debug
        @http = ::HTTP::Server.new(handlers)
      end

      def bind(socket)
        addr = @http.bind(socket)
        @log.info { "Bound to #{addr}" }
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        @log.info { "Bound to #{addr}" }
      end

      def bind_tls(address, port, ctx)
        addr = @http.bind_tls address, port, ctx
        @log.info { "Bound on #{addr}" }
      end

      def bind_unix(path)
        File.delete?(path)
        addr = @http.bind_unix(path)
        File.chmod(path, 0o666)
        @log.info { "Bound to #{addr}" }
      end

      def bind_internal_unix
        File.delete?(INTERNAL_UNIX_SOCKET)
        addr = @http.bind_unix(INTERNAL_UNIX_SOCKET)
        File.chmod(INTERNAL_UNIX_SOCKET, 0o660)
        @log.info { "Bound to #{addr}" }
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
        File.delete?(INTERNAL_UNIX_SOCKET)
      end

      def closed?
        @http.closed?
      end

      class NotFoundError < Exception; end

      class ExpectedBodyError < ArgumentError; end

      class UnknownContentType < Exception; end
    end
  end
end
