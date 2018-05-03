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
      halt(context, 302)
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

    private def not_found(context, message = "Not found")
      halt(context, 404, { error: "not_found", reason: message })
    end

    private def bad_request(context, message = "Bad request")
      halt(context, 400, { error: "bad_request", reason: message })
    end

    private def access_refused(context, message = "Access refused")
      halt(context, 401, { error: "access_refused", reason: message })
    end

    private def halt(context, status_code, body = nil)
      context.response.status_code = status_code
      body.try &.to_json(context.response)
      raise HaltRequest.new
    end

    private def with_vhost(context, params)
      vhost = URI.unescape(params["vhost"])
      if @amqp_server.vhosts[vhost]?
        yield vhost
      else
        not_found(context, "VHost #{vhost} does not exist")
      end
      context
    end

    private def authorized_user(context) : User
      user = nil
      if username = context.authorized_username?
        user = @amqp_server.users[username]?
      end
      unless user
        @log.debug "Authorized user not in user store"
        access_refused(context)
      end
      user
    end
    class HaltRequest < Exception; end
  end
end
