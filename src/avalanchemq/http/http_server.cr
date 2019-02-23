require "http/server"
require "json"
require "logger"
require "router"
require "./handler/*"
require "./json_cache"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module AvalancheMQ
  module HTTP
    class Server
      @running = false

      getter cache

      def initialize(@amqp_server : AvalancheMQ::Server, @port : Int32, @log : Logger)
        @log.progname = "httpserver"
        @cache = JSONCacheHandler.new(@log.dup)
        handlers = [
          ApiDefaultsHandler.new,
          ApiErrorHandler.new(@log.dup),
          StaticController.new,
          BasicAuthHandler.new(@amqp_server.users, @log.dup),
          # @cache,
          MainController.new(@amqp_server, @log.dup).route_handler,
          DefinitionsController.new(@amqp_server, @log.dup).route_handler,
          ConnectionsController.new(@amqp_server, @log.dup).route_handler,
          ChannelsController.new(@amqp_server, @log.dup).route_handler,
          ConsumersController.new(@amqp_server, @log.dup).route_handler,
          ExchangesController.new(@amqp_server, @log.dup).route_handler,
          QueuesController.new(@amqp_server, @log.dup).route_handler,
          BindingsController.new(@amqp_server, @log.dup).route_handler,
          VHostsController.new(@amqp_server, @log.dup).route_handler,
          UsersController.new(@amqp_server, @log.dup).route_handler,
          PermissionsController.new(@amqp_server, @log.dup).route_handler,
          ParametersController.new(@amqp_server, @log.dup).route_handler,
          NodesController.new(@amqp_server, @log.dup).route_handler,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new) if @log.level == Logger::DEBUG
        @http = ::HTTP::Server.new(handlers)
      end

      def listen
        @running = true
        addr = @http.bind_tcp "::", @port, true
        @log.info { "Listening on #{addr}" }
        @http.listen
      end

      def close
        @http.try &.close
        @running = false
      end

      def closed?
        !@running
      end

      class PayloadTooLarge < Exception; end

      class NotFoundError < Exception; end

      class ExpectedBodyError < ArgumentError; end

      class UnknownContentType < Exception; end
    end
  end
end
