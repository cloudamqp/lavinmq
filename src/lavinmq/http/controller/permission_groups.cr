require "../controller"

module LavinMQ
  module HTTP
    class PermissionGroupsController < Controller
      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/mqtt/permission-groups" do |context, _params|
          refuse_unless_administrator(context, user(context))
          JSON.build(context.response) do |json|
            json.array do
              @server.vhosts.each_value do |vhost|
                vhost.mqtt_permission_service.values.each(&.to_json(json))
              end
            end
          end
          context
        end

        get "/api/mqtt/permission-groups/:vhost" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            vhost.mqtt_permission_service.values.to_json(context.response)
          end
        end

        get "/api/mqtt/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service[params["name"]]?
            not_found(context) unless group
            group.to_json(context.response)
          end
        end

        # Creates an empty group; members and rules are managed through the
        # endpoints below.
        put "/api/mqtt/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            unless parse_body(context).as_h.empty?
              bad_request(context, "Group create takes no body, use the members and rules endpoints")
            end
            service = vhost.mqtt_permission_service
            if service[params["name"]]?
              context.response.status = ::HTTP::Status::NO_CONTENT
            else
              service.put(MQTT::PermissionGroup.new(params["name"], vhost.name))
              context.response.status = ::HTTP::Status::CREATED
            end
          end
        end

        delete "/api/mqtt/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service.delete(params["name"])
            not_found(context) unless group
            context.response.status = ::HTTP::Status::NO_CONTENT
          end
        end

        put "/api/mqtt/permission-groups/:vhost/:name/members/:member" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            service = vhost.mqtt_permission_service
            group = service[params["name"]]?
            not_found(context) unless group
            member = params["member"]
            if group.members.includes?(member)
              context.response.status = ::HTTP::Status::NO_CONTENT
            else
              service.put(MQTT::PermissionGroup.new(group.name, group.vhost, group.members + [member], group.rules))
              context.response.status = ::HTTP::Status::CREATED
            end
          end
        end

        delete "/api/mqtt/permission-groups/:vhost/:name/members/:member" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            service = vhost.mqtt_permission_service
            group = service[params["name"]]?
            not_found(context) unless group
            member = params["member"]
            not_found(context) unless group.members.includes?(member)
            service.put(MQTT::PermissionGroup.new(group.name, group.vhost, group.members - [member], group.rules))
            context.response.status = ::HTTP::Status::NO_CONTENT
          end
        end

        put "/api/mqtt/permission-groups/:vhost/:name/rules/:identifier" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            service = vhost.mqtt_permission_service
            group = service[params["name"]]?
            not_found(context) unless group
            body = parse_body(context)
            begin
              pattern = body["pattern"]?.try(&.as_s?)
              bad_request(context, "Field 'pattern' is required") unless pattern
              rule = MQTT::PermissionGroup::Rule.new(params["identifier"], pattern,
                read: body["read"]?.try(&.as_bool?) || false,
                write: body["write"]?.try(&.as_bool?) || false)
              existing = group.rules.any?(&.identifier.== rule.identifier)
              rules = group.rules.reject(&.identifier.== rule.identifier) << rule
              service.put(MQTT::PermissionGroup.new(group.name, group.vhost, group.members, rules))
              context.response.status = existing ? ::HTTP::Status::NO_CONTENT : ::HTTP::Status::CREATED
            rescue ex : ArgumentError
              bad_request(context, "Invalid rule: #{ex.message}")
            end
          end
        end

        delete "/api/mqtt/permission-groups/:vhost/:name/rules/:identifier" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            service = vhost.mqtt_permission_service
            group = service[params["name"]]?
            not_found(context) unless group
            identifier = params["identifier"]
            not_found(context) unless group.rules.any?(&.identifier.== identifier)
            rules = group.rules.reject(&.identifier.== identifier)
            service.put(MQTT::PermissionGroup.new(group.name, group.vhost, group.members, rules))
            context.response.status = ::HTTP::Status::NO_CONTENT
          end
        end
      end
    end
  end
end
