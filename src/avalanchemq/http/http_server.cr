require "http/server"
require "json"
require "logger"
require "router"
require "./handler/*"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end
module AvalancheMQ
  class HTTPServer
    @log : Logger

    @running = false

    def initialize(@amqp_server : AvalancheMQ::Server, @port : Int32)
      @log = @amqp_server.log.dup
      @log.progname = "httpserver"
    end

    def listen
      @running = true
      handlers = [
        ApiDefaultsHandler.new,
        ApiErrorHandler.new(@log),
        StaticController.new.route_handler,
        BasicAuthHandler.new(@amqp_server.users, @log),
        MainController.new(@amqp_server).route_handler,
        DefinitionsController.new(@amqp_server).route_handler,
        ConnectionsController.new(@amqp_server).route_handler,
        ChannelsController.new(@amqp_server).route_handler,
        ConsumersController.new(@amqp_server).route_handler,
        ExchangesController.new(@amqp_server).route_handler,
        QueuesController.new(@amqp_server).route_handler,
        BindingsController.new(@amqp_server).route_handler,
        VHostsController.new(@amqp_server).route_handler,
        UsersController.new(@amqp_server).route_handler,
        PermissionsController.new(@amqp_server).route_handler,
        ParametersController.new(@amqp_server).route_handler,
      ] of HTTP::Handler
      handlers.unshift(HTTP::LogHandler.new) if @log.level == Logger::DEBUG
      @http = HTTP::Server.new("::", @port, handlers)
      server = @http.not_nil!.bind
      @log.info { "Listening on #{server.local_address}" }
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
