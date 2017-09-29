require "http/server"
require "json"

module AMQPServer
  class HTTPServer
    def initialize(@amqp_server : AMQPServer::Server, port)
      @http = HTTP::Server.new(port) do |context|
        if context.request.method != "GET"
          context.response.status_code = 404
          context.response.print "Method not allowed"
          next
        end
        case context.request.path
        when "/api/connections"
          context.response.content_type = "application/json"
          @amqp_server.connections.to_json(context.response)
        when "/api/exchanges"
          context.response.content_type = "application/json"
          @amqp_server.vhosts.map { |_, v| v.exchanges }.to_json(context.response)
        when "/api/queues"
          context.response.content_type = "application/json"
          @amqp_server.vhosts.map { |_, v| v.queues }.to_json(context.response)
        else
          context.response.content_type = "text/plain"
          context.response.print "AMQP Server"
        end
      end
    end

    def listen
      server = @http.bind
      print "HTTP API listening on ", server.local_address, "\n"
      @http.listen
    end

    def close
      @http.close
    end
  end
end
