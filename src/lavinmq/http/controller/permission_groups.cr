require "../controller"
require "../../sortable_json"

module LavinMQ
  module HTTP
    # Groups are returned as summaries with counts, so a group with thousands
    # of members or rules stays small in every route.
    struct PermissionGroupSummaryView
      include SortableJSON

      def initialize(@group : MQTT::PermissionGroup)
      end

      def details_tuple
        {
          name:         @group.name,
          vhost:        @group.vhost,
          member_count: @group.members.size,
          rule_count:   @group.rules.size,
        }
      end

      protected def search_value
        @group.name
      end
    end

    struct PermissionGroupMemberView
      include SortableJSON

      def initialize(@client_id : String)
      end

      def details_tuple
        {client_id: @client_id}
      end

      protected def search_value
        @client_id
      end
    end

    class PermissionGroupsController < Controller
      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/mqtt/permission-groups" do |context, _params|
          refuse_unless_administrator(context, user(context))
          views = Array(PermissionGroupSummaryView).new
          @server.vhosts.each_value do |vhost|
            vhost.mqtt_permission_service.values.each do |group|
              views << PermissionGroupSummaryView.new(group)
            end
          end
          page(context, views)
        end

        get "/api/mqtt/permission-groups/:vhost" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            views = vhost.mqtt_permission_service.values.map { |g| PermissionGroupSummaryView.new(g) }
            page(context, views)
          end
        end

        get "/api/mqtt/permission-groups/:vhost/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service[params["name"]]?
            not_found(context) unless group
            PermissionGroupSummaryView.new(group).to_json(context.response)
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
              begin
                service.put(MQTT::PermissionGroup.new(params["name"], vhost.name))
              rescue ex : ArgumentError
                bad_request(context, ex.message)
              end
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

        get "/api/mqtt/permission-groups/:vhost/:name/members" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service[params["name"]]?
            not_found(context) unless group
            views = group.members.map { |m| PermissionGroupMemberView.new(m) }
            page(context, views)
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

        get "/api/mqtt/permission-groups/:vhost/:name/rules" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            group = vhost.mqtt_permission_service[params["name"]]?
            not_found(context) unless group
            group.rules.to_json(context.response)
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
