require "http/server"
require "json"
require "logger"
require "router"
require "./handler/*"
require "../json_socket/json_cache"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module AvalancheMQ
  class HTTPServer
    @running = false

    getter cache

    def initialize(@amqp_server : Server, @port : Int32, @log : Logger)
      @log.progname = "httpserver"
      @cache = JSONCacheHandler.new(@log.dup)
    end

    def listen
      @running = true
      handlers = [
        ApiDefaultsHandler.new,
        ApiErrorHandler.new(@log.dup),
        StaticController.new.route_handler,
        BasicAuthHandler.new(@amqp_server.users, @log.dup),
        @cache,
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
      ] of HTTP::Handler
      handlers.unshift(HTTP::LogHandler.new) if @log.level == Logger::DEBUG
      @http = HTTP::Server.new(handlers)
      addr = @http.not_nil!.bind_tcp "::", @port, true
      @log.info { "Listening on #{addr}" }
      @http.not_nil!.listen
    end

    def close
      @http.try &.close
    end

    def closed?
      !@running
    end

    class NotFoundError < Exception; end

    class ExpectedBodyError < ArgumentError; end

    class UnknownContentType < Exception; end
  end
end
