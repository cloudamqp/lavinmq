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
          @server.permission_service.values.to_json(context.response)
          context
        end

        get "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = params["name"]
          group = @server.permission_service[name]?
          not_found(context) unless group
          group.to_json(context.response)
          context
        end

        put "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = params["name"]
          body = parse_body(context)
          begin
            protocol = (raw = present(body, "protocol")) ? String.from_json(raw.to_json) : "mqtt"
            members = (raw = present(body, "members")) ? Array(String).from_json(raw.to_json) : [] of String
            rules = (raw = present(body, "rules")) ? Array(Auth::PermissionGroup::Rule).from_json(raw.to_json) : [] of Auth::PermissionGroup::Rule
            is_update = @server.permission_service[name]?
            @server.permission_service.put(Auth::PermissionGroup.new(name, protocol, members, rules))
            context.response.status = is_update ? ::HTTP::Status::NO_CONTENT : ::HTTP::Status::CREATED
          rescue ex : JSON::Error | ArgumentError
            bad_request(context, "Invalid permission group: #{ex.message}")
          end
          context
        end

        delete "/api/permission-groups/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          group = @server.permission_service.delete(params["name"])
          not_found(context) unless group
          context.response.status = ::HTTP::Status::NO_CONTENT
          context
        end
      end
    end
  end
end
