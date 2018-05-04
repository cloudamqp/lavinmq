require "uri"
require "../controller"
require "../resource_helper"

module AvalancheMQ
  class ExchangesController < Controller
    include ResourceHelper

    private def register_routes
      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end

      get "/api/exchanges/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].exchanges.values.to_json(context.response)
        end
      end

      get "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          name = params["name"]
          user = user(context)
          e = @amqp_server.vhosts[vhost].exchanges[name]?
          not_found(context, "Exchange #{name} does not exist") unless e
          e.to_json(context.response)
        end
      end

      put "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          name = params["name"]
          unless user.can_config?(vhost, name)
            access_refused(context, "User doesn't have permissions to declare exchange '#{name}'")
          end
          body = parse_body(context)
          type = body["type"]?.try &.as_s
          bad_request(context, "Field 'type' is required") unless type
          durable = body["durable"]?.try(&.as_bool?) || false
          auto_delete = body["auto_delete"]?.try(&.as_bool?) || false
          internal = body["internal"]?.try(&.as_bool?) || false
          arguments = parse_arguments(body)
          e = @amqp_server.vhosts[vhost].exchanges[name]?
          if e
            unless e.match?(type, durable, auto_delete, internal, arguments)
              bad_request(context, "Existing exchange declared with other arguments arg")
            end
            context.response.status_code = 200
          elsif name.starts_with? "amq."
            bad_request(context, "Not allowed to use the amq. prefix")
          else
            @amqp_server.vhosts[vhost]
              .declare_exchange(name, type, durable, auto_delete, internal, arguments)
            context.response.status_code = 201
          end
        end
      end

      delete "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          name = params["name"]
          unless user.can_config?(vhost, name)
            access_refused(context, "User doesn't have permissions to delete exchange '#{name}'")
          end
          @amqp_server.vhosts[vhost].delete_exchange(name)
          context.response.status_code = 204
        end
      end
    end
  end
end
