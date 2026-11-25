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
            [VHostLimitsView.new(vhost).self_if_limited].compact.to_json(context.response.output)
          end
        end

        put "/api/vhost-limits/:vhost/:type" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          context.response.status_code = 400
          with_vhost(context, params) do |vhost|
            if body = context.request.body
              json = JSON.parse(body)
              if (value = json["value"]?.try &.as_i64?) && set_limit(vhost, params["type"], value)
                context.response.status_code = 204
              end
            end
          end
          context
        end

        delete "/api/vhost-limits/:vhost/:type" do |context, params|
          context.response.status_code = 400
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            context.response.status_code = 204 if set_limit(vhost, params["type"], nil)
          end
          context
        end
      end

      # Returns false for an unknown limit type
      private def set_limit(vhost : VHost, type : String, value : Int64?) : Bool
        case type
        when "max-connections"           then vhost.max_connections = value.try &.to_i32
        when "max-queues"                then vhost.max_queues = value.try &.to_i32
        when "max-retained-messages"     then vhost.max_retained_messages = value
        when "max-retained-message-size" then vhost.max_retained_message_size = value
        when "retained-message-ttl"      then vhost.retained_message_ttl = value
        else                                  return false
        end
        true
      end

      struct VHostLimitsView
        include SortableJSON

        def initialize(@vhost : VHost)
        end

        def self_if_limited : VHostLimitsView?
          self if @vhost.max_connections || @vhost.max_queues ||
                  @vhost.max_retained_messages || @vhost.max_retained_message_size ||
                  @vhost.retained_message_ttl
        end

        def details_tuple
          value = NamedTuple.new
          if max = @vhost.max_connections
            value = value.merge({"max-connections": max})
          end
          if max = @vhost.max_queues
            value = value.merge({"max-queues": max})
          end
          if max = @vhost.max_retained_messages
            value = value.merge({"max-retained-messages": max})
          end
          if max = @vhost.max_retained_message_size
            value = value.merge({"max-retained-message-size": max})
          end
          if ttl = @vhost.retained_message_ttl
            value = value.merge({"retained-message-ttl": ttl})
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
