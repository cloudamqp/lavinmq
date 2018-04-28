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
      end

      get "/api/connections/:name" do |context, params|
        with_connection(context, params) do |connection|
          connection.to_json(context.response)
        end
      end

      delete "/api/connections/:name" do |context, params|
        with_connection(context, params) { |c| c.close }
      end

      get "/api/connections/:name/channels" do |context, params|
        with_connection(context, params) do |connection|
          connection.channels.values.to_json(context.response)
        end
      end
    end

    private def with_connection(context, params)
      name = URI.unescape(params["name"])
      connection = @amqp_server.connections.find { |c| c.name == name }
      if connection
        yield connection
      else
        not_found(context, "Connection #{name} does not exist")
      end
      context
    end
  end
end
