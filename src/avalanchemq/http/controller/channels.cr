require "uri"
require "../controller"

module AvalancheMQ
  class ChannelsController < Controller
    private def register_routes
      get "/api/channels" do |context, _params|
        @amqp_server.connections.flat_map { |c| c.channels.values }.to_json(context.response)
        context
      end

      get "/api/vhosts/:vhost/channels" do |context, params|
        with_vhost(context, params) do |vhost|
          c = @amqp_server.connections.find { |c| c.vhost.name == vhost }
          if c
            c.channels.values.to_json(context.response)
          else
            context.response.print("[]")
          end
        end
      end
    end
  end
end
