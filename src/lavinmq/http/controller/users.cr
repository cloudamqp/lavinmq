require "../controller"

module LavinMQ
  module HTTP
    struct UserView
      include SortableJSON

      def initialize(@user : User)
      end

      def details_tuple
        @user.user_details
      end
    end

    module UserHelpers
      private def user(context, params, key = "name")
        name = URI.decode_www_form(params[key])
        u = @amqp_server.users[name]?
        not_found(context, "Not Found") if u.nil? || u.hidden?
        u
      end
    end

    class UsersController < Controller
      include UserHelpers

      private def register_routes # ameba:disable Metrics/CyclomaticComplexity
        get "/api/users" do |context, _params|
          refuse_unless_administrator(context, user(context))
          page(context, @amqp_server.users.each_value.reject(&.hidden?)
            .map { |u| UserView.new(u) })
        end

        get "/api/users/without-permissions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          itr = @amqp_server.users.each_value.reject(&.hidden?)
            .select(&.permissions.empty?)
            .map { |u| UserView.new(u) }
          page(context, itr)
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
          bad_request(context, "Illegal user name") if UserStore.hidden?(name)
          body = parse_body(context)
          password_hash = body["password_hash"]?.try &.as_s?
          password = body["password"]?.try &.as_s?
          tags = Tag.parse_list(body["tags"]?.try(&.as_s).to_s).uniq
          hashing_algorithm = body["hashing_algorithm"]?.try &.as_s? || "SHA256"
          unless @amqp_server.flow?
            precondition_failed(context, "Server low on disk space, can not create new user")
          end
          if u = @amqp_server.users[name]?
            if password_hash
              u.update_password_hash(password_hash, hashing_algorithm)
            elsif password
              u.update_password(password)
            end
            u.tags = tags if body["tags"]?
            @amqp_server.users.save!
            context.response.status_code = 204
          else
            if password_hash
              @amqp_server.users.add(name, password_hash, hashing_algorithm, tags)
            elsif password
              @amqp_server.users.create(name, password, tags)
            else
              bad_request(context, "Field 'password_hash' or 'password' is required when creating new user")
            end
            context.response.status_code = 201
          end
          context
        rescue ex : Base64::Error
          bad_request(context, ex.message)
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

        put "/api/auth/hash_password/:password" do |context, params|
          password = params["password"]
          hash = User.hash_password(password, "SHA256")
          {password_hash: hash.to_s}.to_json(context.response)
          context
        end
      end
    end
  end
end
