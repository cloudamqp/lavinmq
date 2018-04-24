require "http/server"
require "json"
require "logger"
require "router"
require "./handler/error_handler"
require "./handler/defaults_handler"
require "./controller"
require "./controller/definitions"
require "./controller/main"

module AvalancheMQ
  class HTTPServer
    @log : Logger
    def initialize(@amqp_server : AvalancheMQ::Server, @port : Int32)
      @log = @amqp_server.log.dup
      @log.progname = "HTTP(#{port})"
    end

    def listen
      @http = HTTP::Server.new(@port, [ApiDefaultsHandler.new,
                                       ApiErrorHandler.new(@log),
                                       MainController.new(@amqp_server).route_handler,
                                       DefinitionsController.new(@amqp_server).route_handler])
      server = @http.not_nil!.bind
      @log.info "Listening on #{server.local_address}"
      @http.not_nil!.listen
    end

    def close
      @http.try &.close
    end

    class NotFoundError < Exception; end
    class ExpectedBodyError < ArgumentError; end
    class UnknownContentType < Exception; end
  end
end
