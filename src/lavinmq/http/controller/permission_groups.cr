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

      def initialize(@username : String)
      end

      def details_tuple
        {username: @username}
      end

      protected def search_value
        @username
      end
    end

    class PermissionGroupsController < Controller
      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/mqtt/permission-groups" do |context, _params|
          refuse_unless_administrator(context, user(context))
          total = @server.vhosts.sum { |_, vhost| vhost.mqtt_permission_service.size }
          views = Array(PermissionGroupSummaryView).new(total)
          @server.vhosts.each_value do |vhost|
            vhost.mqtt_permission_service.each_value do |group|
              views << PermissionGroupSummaryView.new(group)
            end
          end
          page(context, views)
        end

        get "/api/mqtt/permission-groups/:vhost" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            service = vhost.mqtt_permission_service
            views = Array(PermissionGroupSummaryView).new(service.size)
            service.each_value { |group| views << PermissionGroupSummaryView.new(group) }
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
            created =
              begin
                vhost.mqtt_permission_service.create(MQTT::PermissionGroup.new(params["name"], vhost.name))
              rescue ex : ArgumentError
                bad_request(context, ex.message)
              end
            context.response.status = created ? ::HTTP::Status::CREATED : ::HTTP::Status::NO_CONTENT
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

        put "/api/mqtt/permission-groups/:vhost/:name/members/:username" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            member = params["username"]
            added = false
            found = vhost.mqtt_permission_service.update(params["name"]) do |group|
              next if group.members.includes?(member)
              added = true
              MQTT::PermissionGroup.new(group.name, group.vhost, group.members + [member], group.rules)
            end
            not_found(context) unless found
            context.response.status = added ? ::HTTP::Status::CREATED : ::HTTP::Status::NO_CONTENT
          end
        end

        delete "/api/mqtt/permission-groups/:vhost/:name/members/:username" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            member = params["username"]
            removed = false
            vhost.mqtt_permission_service.update(params["name"]) do |group|
              next unless group.members.includes?(member)
              removed = true
              MQTT::PermissionGroup.new(group.name, group.vhost, group.members - [member], group.rules)
            end
            not_found(context) unless removed
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
            # Answer 404 before the body is read, so a client that creates the
            # group on 404 and retries still gets it. The update below gives the
            # authoritative 404, because this check is stale by then.
            not_found(context) unless vhost.mqtt_permission_service[params["name"]]?
            # Read the body before the group: parse_body waits on the socket,
            # and a group read before that wait is stale by the time it is used.
            body = parse_body(context)
            begin
              pattern = body["pattern"]?.try(&.as_s?)
              bad_request(context, "Field 'pattern' is required") unless pattern
              rule = MQTT::PermissionGroup::Rule.new(params["identifier"], pattern,
                read: rule_flag(context, body, "read"),
                write: rule_flag(context, body, "write"))
              existing = false
              found = vhost.mqtt_permission_service.update(params["name"]) do |group|
                existing = group.rules.any?(&.identifier.== rule.identifier)
                rules = group.rules.reject(&.identifier.== rule.identifier) << rule
                MQTT::PermissionGroup.new(group.name, group.vhost, group.members, rules)
              end
              not_found(context) unless found
              context.response.status = existing ? ::HTTP::Status::NO_CONTENT : ::HTTP::Status::CREATED
            rescue ex : ArgumentError
              bad_request(context, "Invalid rule: #{ex.message}")
            end
          end
        end

        delete "/api/mqtt/permission-groups/:vhost/:name/rules/:identifier" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            identifier = params["identifier"]
            removed = false
            vhost.mqtt_permission_service.update(params["name"]) do |group|
              next unless group.rules.any?(&.identifier.== identifier)
              removed = true
              rules = group.rules.reject(&.identifier.== identifier)
              MQTT::PermissionGroup.new(group.name, group.vhost, group.members, rules)
            end
            not_found(context) unless removed
            context.response.status = ::HTTP::Status::NO_CONTENT
          end
        end
      end

      # An absent flag is false. A present flag must be a boolean: coercing
      # "true" to false would create a rule that grants nothing and report
      # success.
      private def rule_flag(context, body : JSON::Any, field : String) : Bool
        value = body[field]?
        return false if value.nil?
        case flag = value.as_bool?
        in Bool then flag
        in Nil  then bad_request(context, "Field '#{field}' must be a boolean")
        end
      end
    end
  end
end
