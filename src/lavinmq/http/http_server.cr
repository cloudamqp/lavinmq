require "http/server"
require "http-protection"
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
          ::HTTP::Protection::StrictTransport.new,
          ::HTTP::Protection::FrameOptions.new,
          AMQPWebsocket.new(@amqp_server),
          ApiDefaultsHandler.new,
          ApiErrorHandler.new(@log),
          StaticController.new,
          BasicAuthHandler.new(@amqp_server, @log),
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
          PrometheusController.new(@amqp_server, @log).route_handler,
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
        addr = @http.bind_unix path
        File.chmod(path, 0o777)
        @log.info { "Bound to #{addr}" }
      end

      def bind_internal_unix
        if File.exists? INTERNAL_UNIX_SOCKET
          File.delete INTERNAL_UNIX_SOCKET
        else
          FileUtils.mkdir_p(File.dirname(INTERNAL_UNIX_SOCKET))
        end
        addr = @http.bind_unix INTERNAL_UNIX_SOCKET
        File.chmod(INTERNAL_UNIX_SOCKET, 0o770)
        @log.info { "Bound to #{addr}" }
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
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
