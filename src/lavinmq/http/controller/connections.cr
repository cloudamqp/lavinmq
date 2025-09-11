require "uri"
require "../controller"

module LavinMQ
  module HTTP
    module ConnectionsHelper
      private def connections(user : Auth::User)
        if user.tags.any? { |t| t.administrator? || t.monitoring? }
          @amqp_server.connections
        else
          vhosts = user.permitted_vhosts
          @amqp_server.connections.select &.vhost.name.in?(vhosts)
        end
      end
    end

    class ConnectionsController < Controller
      include ConnectionsHelper

      private def register_routes
        get "/api/connections" do |context, _params|
          page(context, connections(user(context)))
        end

        get "/api/vhosts/:vhost/connections" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].connections)
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
            page(context, connection.channels)
          end
        end

        get "/api/connections/username/:username" do |context, params|
          connections = get_connections_by_username(context, params["username"])
          page(context, connections)
        end

        delete "/api/connections/username/:username" do |context, params|
          connections = get_connections_by_username(context, params["username"])
          reason = context.request.headers["X-Reason"]? || "Closed via management plugin"
          connections.each do |c|
            c.close(reason)
          end
          context.response.status_code = 204
          context
        end
      end

      private def get_connections_by_username(context, username)
        username = URI.decode_www_form(username)
        user = user(context)
        connections(user).select { |c| c.user.name == username }
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

      private def can_access_connection?(c : Client, user : Auth::User) : Bool
        c.user == user || user.tags.any? { |t| t.administrator? || t.monitoring? }
      end
    end
  end
end
