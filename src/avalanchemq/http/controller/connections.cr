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
        with_vhost(context, params) do |vhost|
          @amqp_server.connections.select { |c| c.vhost.name == vhost }.to_json(context.response)
        end
        context
      end

      get "/api/connections/:name" do |context, params|
        name = URI.unescape(params["name"])
        connection = @amqp_server.connections.find { |c| c.name == name }
        if connection
          connection.to_json(context.response)
        else
          not_found(context, "Connection #{name} does not exist")
        end
        context
      end
    end
  end
end
