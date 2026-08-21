require "../controller"

module LavinMQ
  module HTTP
    class PermissionGroupsController < Controller
      # A key that is absent, or explicitly JSON null, keeps the field's
      # default.
      private def present(obj, key : String) : JSON::Any?
        raw = obj[key]?
        return if raw.nil? || raw.raw.nil?
        raw
      end

      private def register_routes
        get "/api/permission-groups" do |context, _params|
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

        get "/api/permission-groups/:vhost" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            vhost.mqtt_permission_service.values.to_json(context.response)
          end
        end

        get "/api/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service[params["name"]]?
            not_found(context) unless group
            group.to_json(context.response)
          end
        end

        put "/api/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            name = params["name"]
            body = parse_body(context)
            begin
              members = (raw = present(body, "members")) ? Array(String).from_json(raw.to_json) : Array(String).new
              rules = (raw = present(body, "rules")) ? Array(MQTT::PermissionGroup::Rule).from_json(raw.to_json) : Array(MQTT::PermissionGroup::Rule).new
              service = vhost.mqtt_permission_service
              is_update = service[name]?
              service.put(MQTT::PermissionGroup.new(name, vhost.name, members, rules))
              context.response.status = is_update ? ::HTTP::Status::NO_CONTENT : ::HTTP::Status::CREATED
            rescue ex : JSON::Error | ArgumentError
              bad_request(context, "Invalid permission group: #{ex.message}")
            end
          end
        end

        delete "/api/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service.delete(params["name"])
            not_found(context) unless group
            context.response.status = ::HTTP::Status::NO_CONTENT
          end
        end
      end
    end
  end
end
