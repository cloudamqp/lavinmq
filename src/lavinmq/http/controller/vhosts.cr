require "../controller"
require "../../sortable_json"

module LavinMQ
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
        static_view("/vhosts")
        static_view("/vhost")

        get "/api/vhosts" do |context, _params|
          vhosts = vhosts(user(context)).map { |v| VHostView.new(v) }
          page(context, vhosts)
        end

        get "/api/vhosts/:name" do |context, params|
          with_vhost(context, params, "name") do |vhost|
            refuse_unless_management(context, user(context), vhost)
            v = @amqp_server.vhosts[vhost]
            VHostView.new(v).to_json(context.response)
          end
        end

        put "/api/vhosts/:name" do |context, params|
          u = user(context)
          refuse_unless_administrator(context, u)
          name = URI.decode_www_form(params["name"])
          is_update = @amqp_server.vhosts[name]?
          @amqp_server.vhosts.create(name, u)
          context.response.status_code = is_update ? 204 : 201
          context
        end

        delete "/api/vhosts/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params, "name") do |vhost|
            @amqp_server.vhosts.delete(vhost)
            context.response.status_code = 204
          end
        end

        get "/api/vhosts/:name/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params, "name") do |vhost|
            @amqp_server.users.compact_map do |_, u|
              next if u.hidden?
              u.permissions[vhost]?.try { |p| u.permissions_details(vhost, p) }
            end.to_json(context.response)
          end
        end

        post "/api/vhosts/:name/purge_and_close_consumers" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params, "name") do |vhost|
            v = @amqp_server.vhosts[vhost]?
            not_found(context, "Not Found") unless v
            body = parse_body(context)
            backup = true == body["backup"]?
            dir_name = body["backup_dir_name"]?.to_s
            dir_name = Time.utc.to_unix.to_s if dir_name.empty?
            unless dir_name.match(/^[a-z0-9]+$/)
              bad_request(context, "Bad backup dir name, use only lower case letters and numbers")
            end
            v.purge_queues_and_close_consumers(backup, dir_name)
            context.response.status_code = 204
          end
        end
      end
    end
  end
end
