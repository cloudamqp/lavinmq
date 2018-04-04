require "http/server"
require "json"

module AvalancheMQ
  class HTTPServer
    def initialize(@amqp_server : AvalancheMQ::Server, port)
      @http = HTTP::Server.new(port) do |context|
        context.response.content_type = "application/json"
        case context.request.method
        when "GET"
          get(context)
        when "POST"
          post(context)
        else
          context.response.content_type = "text/plain"
          context.response.status_code = 405
          context.response.print "Method not allowed"
          next
        end
      rescue e : NotFoundError
        not_found(context, e.message)
      end
    end

    def get(context)
      case context.request.path
      when "/api/connections"
        @amqp_server.connections.to_json(context.response)
      when "/api/exchanges"
        @amqp_server.vhosts.flat_map { |_, v| v.exchanges.values }.to_json(context.response)
      when "/api/queues"
        @amqp_server.vhosts.flat_map { |_, v| v.queues.values }.to_json(context.response)
      when "/api/policies"
        @amqp_server.vhosts.flat_map { |_, v| v.policies.values }.to_json(context.response)
      when "/"
        context.response.content_type = "text/plain"
        context.response.print "AvalancheMQ"
      else
        not_found(context)
      end
    end

    def post(context)
      case context.request.path
      when "/api/policies"
        body = JSON.parse(context.request.body.not_nil!)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
        raise NotFoundError.new("No vhost named #{body["vhost"]}") unless vhost
        vhost.add_policy(Policy.from_json(vhost, body))
      else
        not_found(context)
      end
    rescue e : JSON::Error | ArgumentError
      context.response.status_code = 400
      context.response.write({ error: e.message }.to_json.to_slice)
    end

    def not_found(context, message = nil)
      context.response.content_type = "text/plain"
      context.response.status_code = 404
      context.response.print "Not found\n"
      context.response.print message
    end

    def listen
      server = @http.bind
      print "HTTP API listening on ", server.local_address, "\n"
      @http.listen
    end

    def close
      @http.close
    end

    class NotFoundError < Exception; end
  end
end
