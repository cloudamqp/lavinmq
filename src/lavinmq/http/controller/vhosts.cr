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
          if context.request.body
            body = parse_body(context)
            tags = body["tags"]?.to_s.split(',').map(&.strip).reject(&.empty?)
            description = body["description"]?.to_s
          else
            tags = Array(String).new(0)
            description = ""
          end
          is_update = @amqp_server.vhosts[name]?
          if name.bytesize > UInt8::MAX
            bad_request(context, "Vhost name too long, can't exceed 255 characters")
          end
          @amqp_server.vhosts.create(name, u, description, tags)
          context.response.status_code = is_update ? 204 : 201
          context
        end

        delete "/api/vhosts/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params, "name") do |vhost|
            @amqp_server.vhosts.each_value(&.reset_stats) # Reset all vhosts to maintain accurate metrics after deletion
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
      end
    end
  end
end
