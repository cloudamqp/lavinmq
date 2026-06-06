require "../controller"
require "../../sortable_json"

module LavinMQ
  module HTTP
    class VHostLimitsController < Controller
      private def register_routes
        get "/api/vhost-limits" do |context, _params|
          arr = vhosts(user(context)).compact_map { |v| VHostLimitsView.new(v).self_if_limited }
          page(context, arr)
        end

        get "/api/vhost-limits/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            [VHostLimitsView.new(vhost).self_if_limited].compact.to_json(context.response)
          end
        end

        put "/api/vhost-limits/:vhost/:type" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          context.response.status_code = 400
          with_vhost(context, params) do |vhost|
            if body = context.request.body
              value = JSON.parse(body)["value"]?
              context.response.status_code = 204 if set_limit(vhost, params["type"], value)
            end
          end
          context
        end

        delete "/api/vhost-limits/:vhost/:type" do |context, params|
          context.response.status_code = 400
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            case params["type"]
            when "max-connections"
              context.response.status_code = 204
              vhost.max_connections = nil
            when "max-queues"
              context.response.status_code = 204
              vhost.max_queues = nil
            when "sparkplug-aware"
              context.response.status_code = 204
              vhost.sparkplug_aware = false
            end
          end
          context
        end
      end

      # Applies a single vhost limit/flag from the management API.
      # Returns true if a known limit was set, false otherwise.
      private def set_limit(vhost : VHost, type : String, value : JSON::Any?) : Bool
        case type
        when "max-connections"
          if i = value.try &.as_i?
            vhost.max_connections = i < 0 ? -1 : i
            return true
          end
        when "max-queues"
          if i = value.try &.as_i?
            vhost.max_queues = i < 0 ? -1 : i
            return true
          end
        when "sparkplug-aware"
          unless (b = value.try &.as_bool?).nil?
            vhost.sparkplug_aware = b
            return true
          end
        end
        false
      end

      struct VHostLimitsView
        include SortableJSON

        def initialize(@vhost : VHost)
        end

        def self_if_limited : VHostLimitsView?
          return self if @vhost.max_connections || @vhost.max_queues || @vhost.sparkplug_aware?
        end

        def details_tuple
          value = NamedTuple.new
          if max = @vhost.max_connections
            value = value.merge({"max-connections": max})
          end
          if max = @vhost.max_queues
            value = value.merge({"max-queues": max})
          end
          if @vhost.sparkplug_aware?
            value = value.merge({"sparkplug-aware": true})
          end
          {
            vhost: @vhost.name,
            value: value,
          }
        end

        def search_match?(value : String) : Bool
          @vhost.name.includes? value
        end

        def search_match?(value : Regex) : Bool
          value === @vhost.name
        end
      end
    end
  end
end
