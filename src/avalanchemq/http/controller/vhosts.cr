require "../controller"
require "../../sortable_json"

module AvalancheMQ
  module HTTP
    struct VHostView
      include SortableJSON

      def initialize(@vhost : VHost)
      end

      def details_tuple
        @vhost.details_tuple.merge(@vhost.message_details)
      end
    end

    class VHostsController < Controller
      private def register_routes
        get "/api/vhosts" do |context, _params|
          vhosts = vhosts(user(context)).map { |v| VHostView.new(v) }
          page(context, vhosts)
        end

        get "/api/vhosts/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            v = @amqp_server.vhosts[vhost]
            VHostView.new(v).to_json(context.response)
          end
        end

        put "/api/vhosts/:name" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          @amqp_server.vhosts.create(URI.decode_www_form(params["name"]), u)
          context.response.status_code = 204
          context
        end

        delete "/api/vhosts/:vhost" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            @amqp_server.vhosts.delete(vhost)
            context.response.status_code = 204
          end
        end

        get "/api/vhosts/:vhost/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            @amqp_server.users.map do |_, u|
              u.permissions[vhost]?.try { |p| u.permissions_details(vhost, p) }
            end.compact.to_json(context.response)
          end
        end
      end
    end
  end
end
