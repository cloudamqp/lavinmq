require "uri"
require "../controller"

module AvalancheMQ
  class ChannelsController < Controller
    private def register_routes
      get "/api/channels" do |context, _params|
        all_channels.to_json(context.response)
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

      get "/api/channels/:name" do |context, params|
        with_channel(context, params) do |channel|
          channel.details.merge({
            consumer_details: channel.consumers
          }).to_json(context.response)
        end
      end
    end

    private def all_channels
      @amqp_server.connections.flat_map { |c| c.channels.values }
    end

    private def with_channel(context, params)
      name = URI.unescape(params["name"])
      channel = all_channels.find { |c| c.name == name }
      not_found(context, "Channel #{name} does not exist") unless channel
      yield channel
      context
    end
  end
end
