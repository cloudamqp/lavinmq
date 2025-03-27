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
            consumer = find_consumer_from_params(user, vhost, params)
            unless consumer
              context.response.status_code = 404
              break
            end
            consumer.cancel
            context.response.status_code = 204
          end
          context
        end

        put "/api/consumers/:vhost/:connection/:channel/:consumer_tag" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            consumer = find_consumer_from_params(user, vhost, params)
            unless consumer
              context.response.status_code = 404
              break
            end
            body = parse_body(context)
            if prefetch = body["prefetch"]?.try(&.as_i?)
              prefetch = 0 if prefetch < 0
              prefetch = UInt16::MAX if prefetch > UInt16::MAX
              consumer.prefetch_count = prefetch.to_u16
            end
            context.response.status_code = 204
          end
          context
        end
      end

      private def find_consumer_from_params(user, vhost, params)
        consumer_tag = URI.decode_www_form(params["consumer_tag"])
        conn_id = URI.decode_www_form(params["connection"])
        ch_id = URI.decode_www_form(params["channel"]).to_i
        connection = connections(user).find { |conn| conn.vhost.name == vhost && conn.name == conn_id }
        return unless connection
        channel = connection.channels[ch_id]?
        return unless channel
        channel.consumers.find(&.tag.==(consumer_tag))
      end

      private def all_consumers(user)
        Iterator(Client::Channel::Consumer)
          .chain(connections(user).map { |c| c.channels.each_value.flat_map &.consumers })
      end
    end
  end
end
