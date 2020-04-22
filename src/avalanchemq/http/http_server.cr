require "http/server"
require "http-protection"
require "json"
require "log"
require "router"
require "./handler/*"
require "./controller"
require "./controller/*"

class HTTP::Server::Context
  property? authenticated_username : String?
end

module AvalancheMQ
  module HTTP
    class Server
      Log = ::Log.for(self)

      def initialize(@amqp_server : AvalancheMQ::Server)
        handlers = [
          ::HTTP::Protection::StrictTransport.new,
          ::HTTP::Protection::FrameOptions.new,
          ApiDefaultsHandler.new,
          ApiErrorHandler.new,
          StaticController.new,
          BasicAuthHandler.new(@amqp_server.users),
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
          NodesController.new(@amqp_server).route_handler,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new) if Config.instance.log_level == ::Log::Severity::Debug
        @http = ::HTTP::Server.new(handlers)
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        Log.info { "Bound to #{addr}" }
      end

      def bind_tls(address, port, cert_path, key_path)
        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = cert_path
        ctx.private_key = key_path
        addr = @http.bind_tls address, port, ctx
        Log.info { "Bound on #{addr}" }
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

      class PayloadTooLarge < Exception; end

      class NotFoundError < Exception; end

      class ExpectedBodyError < ArgumentError; end

      class UnknownContentType < Exception; end
    end
  end
end
