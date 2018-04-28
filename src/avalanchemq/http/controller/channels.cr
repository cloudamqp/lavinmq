require "uri"
require "../controller"

module AvalancheMQ
  class ChannelsController < Controller
    private def register_routes
      get "/api/channels" do |context, _params|
        @amqp_server.connections.flat_map { |c| c.channels.values }.to_json(context.response)
        context
      end
    end
  end
end
