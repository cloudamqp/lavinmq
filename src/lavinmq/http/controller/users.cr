require "../controller"

module LavinMQ
  module HTTP
    struct UserView
      include SortableJSON

      def initialize(@user : Auth::User)
      end

      def details_tuple
        @user.user_details
      end

      def search_match?(value : String) : Bool
        @user.name.includes? value
      end

      def search_match?(value : Regex) : Bool
        value === @user.name
      end
    end

    module UserHelpers
      private def user(context, params, key = "name")
        name = params[key]
        u = @server.users[name]?
        not_found(context, "Not Found") if u.nil? || u.hidden?
        u
      end

      # A user scoped to `vhost`
      private def vhost_user(context, params, vhost : VHost, key = "name")
        name = params[key]
        u = @server.users[name, vhost.name]?
        not_found(context, "Not Found") if u.nil?
        u
      end
    end

    class UsersController < Controller
      include UserHelpers

      private def register_routes
        get "/api/users" do |context, _params|
          refuse_unless_administrator(context, user(context))
          page(context, @server.users.values.reject(&.hidden?)
            .map { |u| UserView.new(u) })
        end

        get "/api/users/without-permissions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          arr = @server.users.values.reject(&.hidden?)
            .select(&.permissions.empty?)
            .map { |u| UserView.new(u) }
          page(context, arr)
        end

        post "/api/users/bulk-delete" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          users = body["users"]?
          unless users.try &.as_a?
            bad_request(context, "Field 'users' is required")
          end
          users.try &.as_a.each { |u| bulk_delete_user(context, u) }
          @server.users.save_all!
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
          upsert_user(context, params["name"], parse_body(context))
        end

        delete "/api/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          u = user(context, params)
          @server.users.delete(u.name)
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

        # Users scoped to a single vhost. They are managed separately from
        # global users as their names only have to be unique within the vhost.
        get "/api/vhosts/:vhost/users" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            page(context, @server.users.scoped_users(vhost.name).map { |u| UserView.new(u) })
          end
        end

        get "/api/vhosts/:vhost/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            vhost_user(context, params, vhost).user_details.to_json(context.response)
          end
        end

        # Creates or updates a vhost scoped user. On creation the user is given
        # permissions on its vhost from the optional 'configure', 'read' and
        # 'write' fields, defaulting to full access.
        put "/api/vhosts/:vhost/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            body = parse_body(context)
            perms = parse_permission_fields?(context, body) || {/.*/, /.*/, /.*/}
            upsert_user(context, params["name"], body, vhost) do |u|
              @server.users.add_permission(u, vhost.name, *perms)
            end
          end
        end

        delete "/api/vhosts/:vhost/users/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = vhost_user(context, params, vhost)
            @server.users.delete(u.name, vhost: vhost.name)
            context.response.status_code = 204
          end
        end

        # Returns a list, like the global endpoint, even though a scoped user
        # can have at most one entry (on its own vhost)
        get "/api/vhosts/:vhost/users/:name/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            vhost_user(context, params, vhost).permissions_details.to_json(context.response)
          end
        end

        put "/api/vhosts/:vhost/users/:name/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = vhost_user(context, params, vhost)
            perms = parse_permission_fields(context, parse_body(context))
            is_update = u.permissions[vhost.name]?
            @server.users.add_permission(u, vhost.name, *perms)
            context.response.status_code = is_update ? 204 : 201
          end
        end

        delete "/api/vhosts/:vhost/users/:name/permissions" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            u = vhost_user(context, params, vhost)
            @server.users.rm_permission(u, vhost.name)
            context.response.status_code = 204
          end
        end

        put "/api/auth/hash_password" do |context, _params|
          body = parse_body(context)
          if password = body["password"]?.try &.as_s?
            hash = Auth::User.hash_password(password, "SHA256")
            {password_hash: hash.to_s}.to_json(context.response)
            context
          else
            bad_request(context, "Field 'password' is required")
          end
        end
      end

      # Creates or updates a (global or vhost scoped) user from the request
      # body. The block is called with newly created users.
      private def upsert_user(context, name : String, body : JSON::Any, vhost : VHost? = nil, &)
        bad_request(context, "Illegal user name") if Auth::UserStore.hidden?(name)
        password_hash = parse_password_hash(context, body)
        password = body["password"]?.try &.as_s?
        vhost_name = vhost.try &.name
        tags = parse_tags(body, vhost_name)
        hashing_algorithm = body["hashing_algorithm"]?.try &.as_s? || "SHA256"
        unless @server.flow?
          precondition_failed(context, "Server low on disk space, can not create new user")
        end
        if u = @server.users[name, vhost_name]?
          if password_hash
            u.update_password_hash(password_hash, hashing_algorithm)
          elsif password
            u.update_password(password)
          end
          u.tags = tags if body["tags"]?
          @server.users.save!(vhost_name)
          context.response.status_code = 204
        else
          u = if password_hash
                @server.users.add(name, password_hash, hashing_algorithm, tags, vhost: vhost_name)
              elsif password
                @server.users.create(name, password, tags, vhost: vhost_name)
              else
                bad_request(context, "Field 'password_hash' or 'password' is required when creating new user")
              end
          yield u
          context.response.status_code = 201
        end
        context
      rescue ex : Base64::Error
        bad_request(context, ex.message)
      rescue ex : Auth::InvalidPasswordHash | Auth::UserStore::VHostScopeError
        bad_request(context, ex.message)
      end

      # Global users are given by name, vhost scoped users as {"name": .., "vhost": ..}
      private def bulk_delete_user(context, u : JSON::Any)
        if name = u.as_s?
          if @server.users[name]?.nil? && @server.users.values.any? { |x| x.name == name && x.vhost_scoped? }
            bad_request(context, "User '#{name}' is scoped to a vhost, delete it as {\"name\": .., \"vhost\": ..}")
          end
          @server.users.delete(name, false)
        elsif (h = u.as_h?) && (name = h["name"]?.try &.as_s?) && (vhost = h["vhost"]?.try &.as_s?)
          @server.users.delete(name, false, vhost: vhost)
        else
          bad_request(context, "Field 'users' must be array of user names or {\"name\": .., \"vhost\": ..} objects")
        end
      end

      # Users scoped to a vhost can't be given tags with access across vhosts
      private def parse_tags(body : JSON::Any, vhost : String?) : Array(Tag)
        tags = Tag.parse_list(body["tags"]?.try(&.as_s).to_s).uniq
        Auth::UserStore.validate_scoped_tags!(tags) if vhost && body["tags"]?
        tags
      end

      private def parse_password_hash(context, body : JSON::Any) : String?
        if raw_ph = body["password_hash"]?
          bad_request(context, "Field 'password_hash' must be a string or null") unless raw_ph.raw.nil? || raw_ph.raw.is_a?(String)
        end
        body["password_hash"]?.try &.as_s?
      end

      private def upsert_user(context, name : String, body : JSON::Any, vhost : VHost? = nil)
        upsert_user(context, name, body, vhost) { }
      end

      # Parses the 'configure', 'read' and 'write' regex fields, nil if none are given
      private def parse_permission_fields?(context, body : JSON::Any) : Tuple(Regex, Regex, Regex)?
        return if body["configure"]?.nil? && body["read"]?.nil? && body["write"]?.nil?
        parse_permission_fields(context, body)
      end

      # Parses the required 'configure', 'read' and 'write' regex fields
      private def parse_permission_fields(context, body : JSON::Any) : Tuple(Regex, Regex, Regex)
        config = body["configure"]?.try &.as_s?
        read = body["read"]?.try &.as_s?
        write = body["write"]?.try &.as_s?
        unless config && read && write
          bad_request(context, "Fields 'configure', 'read' and 'write' are required")
        end
        {Regex.new(config), Regex.new(read), Regex.new(write)}
      rescue ArgumentError
        bad_request(context, "Permissions must be valid Regex")
      end
    end
  end
end
