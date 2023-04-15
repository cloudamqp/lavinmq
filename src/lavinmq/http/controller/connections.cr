require "uri"
require "../controller"

module LavinMQ
  module HTTP
    module ConnectionsHelper
      private def connections(user : User)
        if user.tags.any? { |t| t.administrator? || t.monitoring? }
          @amqp_server.connections
        else
          vhosts = user.permissions.keys
          @amqp_server.connections.select &.vhost.name.in?(vhosts)
        end
      end
    end

    class ConnectionsController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/connections" do |context, _params|
          page(context, connections(user(context)).each)
        end

        get "/api/vhosts/:vhost/connections" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].connections.each)
          end
        end

        get "/api/connections/:name" do |context, params|
          with_connection(context, params) do |connection|
            connection.to_json(context.response)
          end
        end

        delete "/api/connections/:name" do |context, params|
          with_connection(context, params) do |c|
            reason = context.request.headers["X-Reason"]? || "Closed via management plugin"
            c.close(reason)
            context.response.status_code = 204
          end
        end

        get "/api/connections/:name/channels" do |context, params|
          with_connection(context, params) do |connection|
            page(context, connection.channels.each_value)
          end
        end
      end

      private def with_connection(context, params, &)
        name = URI.decode_www_form(params["name"])
        user = user(context)
        connection = @amqp_server.connections.find { |c| c.name == name }
        not_found(context, "Connection #{name} does not exist") unless connection
        access_refused(context) unless can_access_connection?(connection, user)
        yield connection
        context
      end

      private def can_access_connection?(c : Client, user : User) : Bool
        c.user == user || user.tags.any? { |t| t.administrator? || t.monitoring? }
      end
    end
  end
end
