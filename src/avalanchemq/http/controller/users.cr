require "../controller"

module AvalancheMQ
  module HTTP
    module UserHelpers
      private def user(context, params, key = "name")
        name = URI.unescape(params[key])
        u = @amqp_server.users[name]?
        not_found(context, "User #{name} does not exist") unless u
        u
      end
    end

    class UsersController < Controller
      include UserHelpers

      private def register_routes
        get "/api/users" do |context, _params|
          query = query_params(context)
          refuse_unless_administrator(context, user(context))
          page(query, @amqp_server.users.values.map(&.user_details)).to_json(context.response)
          context
        end

        get "/api/users/without-permissions" do |context, _params|
          query = query_params(context)
          refuse_unless_administrator(context, user(context))
          page(query, @amqp_server.users.values.select { |u| u.permissions.empty? }
            .map(&.user_details)).to_json(context.response)
          context
        end

        post "/api/users/bulk-delete" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          users = body["users"]?
          unless users.try &.as_a?
            bad_request(context, "Field 'users' is required")
          end
          users.try &.as_a.each do |u|
            unless u.as_s?
              bad_request(context, "Field 'users' must be array of user names")
            end
            @amqp_server.users.delete(u.as_s, false)
          end
          context.response.status_code = 204
          context
        end

        get "/api/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          user(context, params).user_details.to_json(context.response)
          context
        end

        put "/api/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = params["name"]
          u = @amqp_server.users[name]?
          body = parse_body(context)
          password_hash = body["password_hash"]?.try &.as_s?
          password = body["password"]?.try &.as_s?
          tags = body["tags"]?.try(&.as_s).to_s.split(",").map { |t| Tag.parse?(t) }.compact
          hashing_alogrithm = body["hashing_alogrithm"]?.try &.as_s? || "SHA256"
          if u
            if password_hash
              u.update_password_hash(password_hash, hashing_alogrithm)
            elsif password
              u.update_password(password)
            else
              bad_request(context, "Field  'password_hash' or 'password' is required when updating existing user")
            end
            u.tags = tags if body["tags"]?
          else
            if password_hash
              @amqp_server.users.add(name, password_hash, hashing_alogrithm, tags)
            elsif password
              @amqp_server.users.create(name, password, tags)
            else
              bad_request(context, "Field 'password_hash' or 'password' is required when creating new user")
            end
            context.response.status_code = 204
          end
          context
        rescue ex : User::InvalidPasswordHash
          bad_request(context, ex.message)
        end

        delete "/api/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          u = user(context, params)
          @amqp_server.users.delete(u.name)
          context.response.status_code = 204
          context
        end

        get "/api/users/:name/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          u = user(context, params)
          if vhost = params["vhost"]?
            u.permissions_details.find { |p| p[:vhost] == vhost }.to_json(context.response)
          else
            u.permissions_details.to_json(context.response)
          end
          context
        end
      end
    end
  end
end
