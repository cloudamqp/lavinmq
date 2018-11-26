require "uri"
require "../controller"
require "./connections"

module AvalancheMQ
  module HTTP
    class ChannelsController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/channels" do |context, _params|
          query = query_params(context)
          page(query, all_channels(user(context))).to_json(context.response)
          context
        end

        get "/api/vhosts/:vhost/channels" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            query = query_params(context)
            c = @amqp_server.connections.find { |conn| conn.vhost.name == vhost }
            channels = c.try(&.channels.values) || [] of Nil
            page(query, channels).to_json(context.response)
          end
        end

        get "/api/channels/:name" do |context, params|
          with_channel(context, params) do |channel|
            channel.details.merge({
              consumer_details: channel.consumers,
            }).to_json(context.response)
          end
        end
      end

      private def all_channels(user)
        connections(user).flat_map { |c| c.channels.values }
      end

      private def with_channel(context, params)
        name = URI.unescape(params["name"])
        channel = all_channels(user(context)).find { |c| c.name == name }
        not_found(context, "Channel #{name} does not exist") unless channel
        yield channel
        context
      end
    end
  end
end
