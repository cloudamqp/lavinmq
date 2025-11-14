require "../user"

module LavinMQ
  module Auth
    class OAuthUser < User
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      @expires_at : Time
      @authenticator : OAuthAuthenticator

      def initialize(@name, @tags, @permissions, @expires_at, @authenticator)
      end

      def expiration=(time : Time)
        @expires_at = time
      end

      def expired? : Bool
        Time.utc > @expires_at
      end

      def update_secret(new_secret : Bytes) : Bool
        username, tags, permissions, expiration = @authenticator.validate_and_extract_claims(@name, new_secret)

        # Update authorization and expiration (trust new token)
        @tags = tags
        @permissions = permissions
        @expires_at = expiration
        clear_permissions_cache
        true
      rescue
        false
      end

      def can_write?(vhost : String, name : String, cache : PermissionCache) : Bool
        return false if expired?
        super
      end

      def can_read?(vhost : String, name : String) : Bool
        return false if expired?
        super
      end

      def can_config?(vhost : String, name : String) : Bool
        return false if expired?
        super
      end

      def can_impersonate? : Bool
        return false if expired?
        super
      end

      def permissions_details(vhost : String, p : Permissions)
        {
          user:      @name,
          vhost:     vhost,
          configure: p[:config],
          read:      p[:read],
          write:     p[:write],
        }
      end
    end
  end
end
