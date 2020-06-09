require "../controller"
require "./connections"
require "../../client/channel/consumer"

module AvalancheMQ
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
            itr = connections(user).each.select { |conn| conn.vhost.name == vhost }
              .flat_map do |conn|
                conn.channels.each_value.flat_map &.consumers
              end
            page(context, itr)
          end
        end

        delete "/api/consumers/:vhost/:consumer_tag" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumer_tag = URI.decode_www_form(params["consumer_tag"])
            connections(user).each.select { |conn| conn.vhost.name == vhost }.each do |conn|
              conn.channels.each_value do |ch|
                ch.consumers.each do |c|
                  next unless c.tag == consumer_tag
                  c.cancel
                end
              end
            end
          end
          context.response.status_code = 204
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
