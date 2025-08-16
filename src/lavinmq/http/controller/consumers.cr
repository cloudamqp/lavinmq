require "../controller"
require "./connections"
require "../../client/channel/consumer"

module LavinMQ
  module HTTP
    class ConsumersController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/consumers" do |context, _params|
          consumers = Array(AMQP::Channel::Consumer).new
          connections(user(context)).each(&.each_channel { |ch| ch.each_consumer { |c| consumers << c } })
          page(context, consumers)
        end

        get "/api/consumers/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumers = Array(AMQP::Channel::Consumer).new
            connections(user(context)).each
              .select(&.vhost.name.==(vhost))
              .each(&.each_channel { |ch| ch.each_consumer { |c| consumers << c } })
            page(context, consumers)
          end
        end

        delete "/api/consumers/:vhost/:connection/:channel/:consumer_tag" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumer_tag = URI.decode_www_form(params["consumer_tag"])
            conn_id = URI.decode_www_form(params["connection"])
            ch_id = URI.decode_www_form(params["channel"]).to_u16
            connection = connections(user).find { |conn| conn.vhost.name == vhost && conn.name == conn_id }
            unless connection
              context.response.status_code = 404
              break
            end
            channel = connection.fetch_channel(ch_id)
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
    end
  end
end
