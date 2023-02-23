require "uri"
require "../controller"
require "./connections"

module LavinMQ
  module HTTP
    class ChannelsController < Controller
      include ConnectionsHelper

      private def register_routes
        static_view "/channels"
        static_view "/channel"

        get "/api/channels" do |context, _params|
          page(context, all_channels(user(context)))
        end

        get "/api/vhosts/:vhost/channels" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            c = @amqp_server.connections.find { |conn| conn.vhost.name == vhost }
            channels = c.try(&.channels.each_value) || ([] of Client::Channel).each
            page(context, channels)
          end
        end

        get "/api/channels/:name" do |context, params|
          with_channel(context, params) do |channel|
            channel.details_tuple.merge({
              consumer_details: channel.consumers,
            }).to_json(context.response)
          end
        end
      end

      private def all_channels(user)
        Iterator(Client::Channel).chain(connections(user).map(&.channels.each_value))
      end

      private def with_channel(context, params, &)
        name = URI.decode_www_form(params["name"])
        channel = all_channels(user(context)).find { |c| c.name == name }
        not_found(context, "Channel #{name} does not exist") unless channel
        yield channel
        context
      end
    end
  end
end
