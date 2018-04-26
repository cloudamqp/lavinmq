require "uri"
require "../controller"

module AvalancheMQ
  class ConnectionsController < Controller
    private def register_routes
      get "/api/connections" do |context, _params|
        @amqp_server.connections.to_json(context.response)
        context
      end

      get "/api/vhosts/:vhost/connections" do |context, params|
        vhost = URI.unescape(params["vhost"])
        @amqp_server.connections.select { |c| c.vhost.name == vhost }.to_json(context.response)
        context
      end
    end
  end
end
