require "../controller"
require "./connections"
require "../../client/channel/consumer"

module LavinMQ
  module HTTP
    class ConsumersController < Controller
      include ConnectionsHelper

      protected def match_value(value)
        value[:consumer_tag]? || value["consumer_tag"]?
      end

      private def register_routes
        get "/api/consumers" do |context, _params|
          page(context, all_consumers(user(context)))
        end

        get "/api/consumers/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            itr = connections(user).each.select(&.vhost.name.==(vhost))
              .flat_map do |conn|
                conn.channels.each_value.flat_map &.consumers
              end
            page(context, itr)
          end
        end

        delete "/api/consumers/:vhost/:connection/:channel/:consumer_tag" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumer_tag = URI.decode_www_form(params["consumer_tag"])
            conn_id = URI.decode_www_form(params["connection"])
            ch_id = URI.decode_www_form(params["channel"]).to_i
            connection = connections(user).find { |conn| conn.vhost.name == vhost && conn.name == conn_id }
            unless connection
              context.response.status_code = 404
              break
            end
            channel = connection.channels[ch_id]?
            unless channel
              context.response.status_code = 404
              break
            end
            consumer = channel.consumers.find(&.tag.==(consumer_tag))
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
          .chain(connections(user).map { |c| c.channels.each_value.flat_map &.consumers })
      end
    end
  end
end
