require "../user"

module LavinMQ
  module Auth
    # Simple OAuth user for userinfo endpoint authentication
    #
    # Unlike OAuthUser, this doesn't support token refresh (UpdateSecret frame)
    # since opaque tokens from userinfo endpoints can't be verified locally.
    class UserinfoUser < User
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      def initialize(@name : String, @tags : Array(Tag), @permissions : Hash(String, Permissions),
                     @expires_at : Time)
      end

      def expiration=(time : Time)
        @expires_at = time
      end

      def expired? : Bool
        Time.utc > @expires_at
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
