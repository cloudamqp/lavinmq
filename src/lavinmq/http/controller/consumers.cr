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
            page(context, all_consumers(user, vhost))
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
            channel = connection.channel?(ch_id.to_u16)
            unless channel
              context.response.status_code = 404
              break
            end
            consumer = channel.find_consumer(&.tag.==(consumer_tag))
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

      private def all_consumers(user, vhost = nil) : Array(Client::Channel::Consumer)
        consumers = Array(Client::Channel::Consumer).new
        connections(user).each do |connection|
          next if vhost && connection.vhost != vhost
          connection.channels.each do |channel|
            channel.consumers.each do |consumer|
              consumers << consumer
            end
          end
        end
        consumers
      end
    end
  end
end
