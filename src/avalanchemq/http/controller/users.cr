require "../controller"
module AvalancheMQ
  module UserHelpers
    private def user(context, params, key = "name")
      name = params[key]
      u = @amqp_server.users[name]?
      not_found(context, "User #{name} does not exist") unless u
      u
    end
  end

  class UsersController < Controller
    include UserHelpers

    private def register_routes
      get "/api/users" do |context, _params|
        @amqp_server.users.map(&.user_details).to_json(context.response)
        context
      end

      get "/api/users/without-permissions" do |context, _params|
        @amqp_server.users.select { |u| u.permissions.empty? }
          .map(&.user_details).to_json(context.response)
        context
      end

      post "/api/users/bulk-delete" do |context, _params|
        body = parse_body(context)
        users = body["users"]?
        unless users.try &.as_a?
          bad_request(context, "Field 'users' is required")
        end
        users.try &.each do |u|
          unless u.as_s?
            bad_request(context, "Field 'users' must be array of user names")
          end
          @amqp_server.users.delete(u.as_s, false)
        end
        @amqp_server.users.save!
        context.response.status_code = 204
        context
      end

      get "/api/users/:name" do |context, params|
        user(context, params).user_details.to_json(context.response)
        context
      end

      put "/api/users/:name" do |context, params|
        name = params["name"]
        u = @amqp_server.users[name]?
        body = parse_body(context)
        password_hash = body["password_hash"]?.try &.as_s?
        password = body["password"]?.try &.as_s?
        hashing_alogrithm = body["hashing_alogrithm"]?.try &.as_s? || "SHA256"
        if u
          if password_hash
            u.update_password(password_hash, hashing_alogrithm)
          else
            bad_request(context, "Field 'password_hash' is required when updating existing user")
          end
        else
          if password_hash
            @amqp_server.users.add(name, password_hash, hashing_alogrithm)
          elsif password
            @amqp_server.users.create(name, password)
          else
            bad_request(context, "Field 'password_hash' or 'password' is required when creating new user")
          end
          context.response.status_code = 201
        end
        context
      rescue ex : User::InvalidPasswordHash
        bad_request(context, ex.message)
      end

      delete "/api/users/:name" do |context, params|
        u = user(context, params)
        @amqp_server.users.delete(u.name)
        context.response.status_code = 204
        context
      end

      get "/api/users/:name/permissions" do |context, params|
        u = user(context, params)
        u.permissions_details.to_json(context.response)
        context
      end
    end
  end
end
