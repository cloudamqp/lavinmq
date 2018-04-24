require "http/server"
require "json"
require "logger"
require "router"
require "./handler/error_handler"
require "./handler/defaults_handler"
require "./controller"
require "./controller/definitions"

module AvalancheMQ

  class MainController < Controller
    private def register_routes
      get "/api/connections" do |context, _params|
        @amqp_server.connections.to_json(context.response)
        context
      end
      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end
      get "/api/queues" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.queues.values }.to_json(context.response)
        context
      end
      get "/api/policies" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.policies.values }.to_json(context.response)
        context
      end
      get "/api/vhosts" do |context, _params|
        @amqp_server.vhosts.to_json(context.response)
        context
      end
      get "/" do |context, _params|
        context.response.content_type = "text/plain"
        context.response.print "AvalancheMQ"
        context
      end

      post "/api/parameters" do |context, _params|
        p = Parameter.from_json context.request.body.not_nil!
        @amqp_server.add_parameter p
        context
      end
      post "/api/policies" do |context, _params|
        p = Policy.from_json context.request.body.not_nil!
        vhost = @amqp_server.vhosts[p.vhost]? || nil
        raise HTTPServer::NotFoundError.new("No vhost named #{p.vhost}") unless vhost
        vhost.add_policy(p)
        context
      end
      post "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.create(body["name"].as_s)
        context
      end

      delete "/api/policies" do |context, _params|
        body = parse_body(context)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
          raise HTTPServer::NotFoundError.new("No vhost named #{body["vhost"].as_s}") unless vhost
        vhost.delete_policy(body["name"].as_s)
        context
      end
      delete "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.delete(body["name"].as_s)
        context
      end
    end
  end

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
