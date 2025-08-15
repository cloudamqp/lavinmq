require "uri"
require "../controller"
require "./connections"

module LavinMQ
  module HTTP
    class ChannelsController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/channels" do |context, _params|
          page(context, all_channels(user(context)))
        end

        get "/api/vhosts/:vhost/channels" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            channels = [] of Client::Channel
            @amqp_server.connections.each do |conn|
              next unless conn.vhost.name == vhost
              conn.each_channel do |channel|
                channels << channel
              end
            end
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

        put "/api/channels/:name" do |context, params|
          with_channel(context, params) do |channel|
            body = parse_body(context)
            if prefetch = body["prefetch"]?.try(&.as_i?)
              unless 0 <= prefetch <= UInt16::MAX
                bad_request(context, "prefetch must be between 0 and #{UInt16::MAX}")
              end
              channel.prefetch_count = prefetch.to_u16
            end
            context.response.status_code = 204
          end
        end
      end

      private def all_channels(user)
        connections(user).flat_map(&.channels)
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
