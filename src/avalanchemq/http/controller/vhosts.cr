
module AvalancheMQ

  class VHostsController < Controller
    private def register_routes
      get "/api/vhosts" do |context, _params|
        @amqp_server.vhosts.map { |v| v.vhost_details.merge(v.message_details) }
          .to_json(context.response)
        context
      end

      get "/api/vhosts/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          v = @amqp_server.vhosts[vhost]
          v.vhost_details.merge(v.message_details)
            .to_json(context.response)
        end
      end

      put "/api/vhosts/:name" do |context, params|
        @amqp_server.vhosts.create(params["name"])
        context.response.status_code = 201
        context
      end

      delete "/api/vhosts/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts.delete(vhost)
          context.response.status_code = 204
        end
      end

      get "/api/vhosts/:vhost/permissions" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.users.flat_map do |u|
            u.permissions[vhost]
          end.to_json(context.response)
        end
      end
    end
  end
end
