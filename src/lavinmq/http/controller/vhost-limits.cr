require "../controller"
require "../../sortable_json"

module LavinMQ
  module HTTP
    class VHostLimitsController < Controller
      private def register_routes
        get "/api/vhost-limits" do |context, _params|
          vhosts = vhosts(user(context)).compact_map { |v| VHostLimitsView.new(v).self_if_limited }
          page(context, vhosts)
        end

        get "/api/vhost-limits/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            v = @amqp_server.vhosts[vhost]
            [VHostLimitsView.new(v).self_if_limited].compact.to_json(context.response)
          end
        end

        put "/api/vhost-limits/:vhost/:type" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          context.response.status_code = 400
          with_vhost(context, params) do |vhost|
            v = @amqp_server.vhosts[vhost]
            if body = context.request.body
              json = JSON.parse(body)
              if value = json["value"]?.try &.as_i?
                value = nil if value < 0
                case params["type"]
                when "max-connections"
                  v.max_connections = value
                  context.response.status_code = 204
                when "max-queues"
                  v.max_queues = value
                  context.response.status_code = 204
                end
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

        def self_if_limited : VHostLimitsView?
          return self if @vhost.max_connections || @vhost.max_queues
        end

        def details_tuple
          value = NamedTuple.new
          if max = @vhost.max_connections
            value = value.merge({"max-connections": max})
          end
          if max = @vhost.max_queues
            value = value.merge({"max-queues": max})
          end
          {
            vhost: @vhost.name,
            value: value,
          }
        end
      end
    end
  end
end
