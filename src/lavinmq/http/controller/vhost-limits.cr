require "../controller"
require "../../sortable_json"

module LavinMQ
  module HTTP
    class VHostLimitsController < Controller
      private def register_routes
        get "/api/vhost-limits" do |context, _params|
          vhosts = vhosts(user(context)).map { |v| VHostLimitsView.new(v) }
          page(context, vhosts)
        end

        get "/api/vhost-limits/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            [VHostLimitsView.new(vhost)].to_json(context.response)
          end
        end

        put "/api/vhost-limits/:vhost/:type" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          context.response.status_code = 400
          with_vhost(context, params) do |vhost|
            body = JSON.parse(context.body_io)
            if value = body["value"]?.try &.as?(Int32)
              value = nil if value < 0
              context.response.status_code = 204
              case params["type"]
              when "max-connections"
                vhost.max_connections = value
                return
              when "max-queues"
                vhost.max_queues = value
                return
              end
            end
          end
          context
        end

        delete "/api/vhost-limits/:name/:type" do |context, params|
          context.response.status_code = 400
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params, "name") do |vhost|
            v = @amqp_server.vhosts[vhost]
            case params["type"]
            when "max-connections"
              context.response.status_code = 204
              v.max_connections = nil
            when "max-queues"
              context.response.status_code = 204
              v.max_queues = nil
            end
          end
          context
        end
      end

      struct VHostLimitsView
        include SortableJSON

        def initialize(@vhost : VHost)
        end

        def details_tuple
          {
            vhost: @vhost.name,
            value: {
              "max-connections": @vhost.max_connections || -1,
              "max-queues":      @vhost.max_queues || -1,
            }.compact,
          }
        end
      end
    end
  end
end
