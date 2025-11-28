require "../user"

module LavinMQ
  module Auth
    class OAuthUser < User
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      def initialize(@name : String, @tags : Array(Tag), @permissions : Hash(String, Permissions),
                     @expires_at : Time, @authenticator : OAuthAuthenticator)
      end

      def expiration=(time : Time)
        @expires_at = time
      end

      def expired? : Bool
        Time.utc > @expires_at
      end

      def update_secret(new_secret : String)
        claims = @authenticator.verify_token(new_secret)

        # Verify the username matches to prevent token substitution attacks
        if claims.username != @name
          raise JWT::VerificationError.new("Token username mismatch: expected '#{@name}', got '#{claims.username}'")
        end

        # Update authorization and expiration (trust new token)
        @tags = claims.tags
        @permissions = claims.permissions
        @expires_at = claims.expires_at
        clear_permissions_cache
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
