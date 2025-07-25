require "../controller"
require "./users"
require "../../sortable_json"

module LavinMQ
  module HTTP
    struct PermissionsView
      include SortableJSON

      def initialize(@user : User, @vhost : String, @p : User::Permissions)
      end

      def details_tuple
        @user.permissions_details(@vhost, @p)
      end

      def search_match?(value : String) : Bool
        @user.name.includes? value
      end

      def search_match?(value : Regex) : Bool
        value === @user.name
      end
    end

    class PermissionsController < Controller
      include UserHelpers

      private def register_routes
        get "/api/permissions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          itr = @amqp_server.users.each_value.reject(&.hidden?)
            .flat_map { |u| u.permissions.map { |vhost, p| PermissionsView.new(u, vhost, p) } }
            .each
          page(context, itr)
        end

        get "/api/permissions/:vhost/:user" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = user(context, params, "user")
            perm = u.permission?(vhost)
            not_found(context) unless perm
            u.permissions_details(vhost, perm).to_json(context.response)
          end
        end

        put "/api/permissions/:vhost/:user" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = user(context, params, "user")
            body = parse_body(context)
            config = body["configure"]?.try &.as_s?
            read = body["read"]?.try &.as_s?
            write = body["write"]?.try &.as_s?
            unless config && read && write
              bad_request(context, "Fields 'configure', 'read' and 'write' are required")
            end
            user = @amqp_server.users[u.name]
            is_update = user.permission?(vhost)
            @amqp_server.users
              .add_permission(u.name, vhost, Regex.new(config), Regex.new(read), Regex.new(write))
            context.response.status_code = is_update ? 204 : 201
          rescue ex : ArgumentError
            bad_request(context, "Permissions must be valid Regex")
          end
        end

        delete "/api/permissions/:vhost/:user" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = user(context, params, "user")
            @amqp_server.users.rm_permission(u.name, vhost)
            context.response.status_code = 204
          end
        end
      end
    end
  end
end
