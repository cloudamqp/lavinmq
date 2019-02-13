require "../controller"
require "./users"
require "../../sortable_json"

module AvalancheMQ
  module HTTP
    struct PermissionsView
      include SortableJSON

      def initialize(@user : User, @vhost : String, @p : User::Permissions)
      end

      def details_tuple
        @user.permissions_details(@vhost, @p)
      end
    end

    class PermissionsController < Controller
      include UserHelpers

      private def register_routes
        get "/api/permissions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          itr = @amqp_server.users
            .flat_map { |_, u| u.permissions.map { |vhost, p| PermissionsView.new(u, vhost, p) } }
            .each
          page(context, itr)
        end

        get "/api/permissions/:vhost/:user" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |_vhost|
            u = user(context, params, "user")
            u.permissions_details.to_json(context.response)
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
            @amqp_server.users
              .add_permission(u.name, vhost, Regex.new(config), Regex.new(read), Regex.new(write))
            context.response.status_code = 204
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
