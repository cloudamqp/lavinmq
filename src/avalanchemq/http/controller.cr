require "router"
require "logger"

module AvalancheMQ
  abstract class Controller
    include Router

    @log : Logger
    def initialize(@amqp_server : AvalancheMQ::Server)
      @log = @amqp_server.log.dup
      @log.progname += "/#{self.class.name}"
      register_routes
    end

    private abstract def register_routes

    private def redirect_back(context)
      context.response.headers["Location"] = context.request.headers["Referer"]
      context.response.status_code = 301
      context.response.close
    end

    private def parse_body(context)
      raise HTTPServer::ExpectedBodyError.new if context.request.body.nil?
      ct = context.request.headers["Content-Type"]? || nil
      if ct.nil? || ct.empty? || ct == "application/json"
        JSON.parse(context.request.body.not_nil!)
      else
        raise HTTPServer::UnknownContentType.new("Unknown Content-Type: #{ct}")
      end
    end
  end
end
