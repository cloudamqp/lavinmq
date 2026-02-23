require "../controller"
require "./connections"
require "../../client/channel/consumer"

module LavinMQ
  module HTTP
    class ConsumersController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/consumers" do |context, _params|
          page(context, all_consumers(user(context)))
        end

        get "/api/consumers/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            itr = connections(user).each.select(&.vhost.==(vhost))
              .flat_map do |conn|
                conn.channels_each_value.flat_map &.consumers
              end
            page(context, itr)
          end
        end

        delete "/api/consumers/:vhost/:connection/:channel/:consumer_tag" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumer_tag = params["consumer_tag"]
            conn_id = params["connection"]
            ch_id = params["channel"].to_i
            connection = connections(user).find { |conn| conn.vhost == vhost && conn.name == conn_id }
            unless connection
              context.response.status_code = 404
              break
            end
            channel = connection.channels_byid?(ch_id.to_u16)
            unless channel
              context.response.status_code = 404
              break
            end
            consumer = channel.consumers_find(&.tag.==(consumer_tag))
            unless consumer
              context.response.status_code = 404
              break
            end
            consumer.cancel
            context.response.status_code = 204
          end
          context
        end
      end

      private def all_consumers(user)
        Iterator(Client::Channel::Consumer)
          .chain(connections(user).map { |c| c.channels_each_value.flat_map &.consumers })
      end
    end
  end
end
