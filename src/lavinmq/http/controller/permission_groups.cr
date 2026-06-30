require "../controller"
require "./users"
require "../../mqtt/topic_filter_set"

module LavinMQ
  module HTTP
    class PermissionGroupsController < Controller
      include UserHelpers

      private def register_routes
        get "/api/permission-groups" do |context, _params|
          refuse_unless_administrator(context, user(context))
          @server.permission_groups.values.to_json(context.response)
          context
        end

        get "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = params["name"]
          group = @server.permission_groups[name]?
          not_found(context) unless group
          group.to_json(context.response)
          context
        end

        put "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = params["name"]
          body = parse_body(context)
          protocol = body["protocol"]?.try(&.as_s?) || "mqtt"
          unless protocol == "mqtt"
            bad_request(context, "Only protocol 'mqtt' is supported")
          end
          apply_to_all = body["apply_to_all"]?.try(&.as_bool?) || false
          members = body["members"]?.try(&.as_a?.try(&.map(&.as_s))) || [] of String
          rules = (body["rules"]?.try(&.as_a?) || [] of JSON::Any).map do |r|
            pattern = r["pattern"]?.try(&.as_s)
            bad_request(context, "Each rule requires a 'pattern'") unless pattern
            unless MQTT::TopicFilterSet.valid_filter?(pattern)
              bad_request(context, "Invalid MQTT topic filter pattern: #{pattern.inspect}")
            end
            Auth::PermissionGroup::Rule.new(
              pattern,
              r["read"]?.try(&.as_bool?) || false,
              r["write"]?.try(&.as_bool?) || false,
            )
          end
          is_update = @server.permission_groups[name]?
          group = Auth::PermissionGroup.new(name, protocol, apply_to_all, members, rules)
          @server.permission_groups.put(group)
          context.response.status_code = is_update ? 204 : 201
          context
        end

        delete "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          @server.permission_groups.delete(params["name"])
          context.response.status_code = 204
          context
        end
      end
    end
  end
end
