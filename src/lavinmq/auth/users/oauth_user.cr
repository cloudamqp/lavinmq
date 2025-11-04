require "../user"

module LavinMQ
  module Auth
    class OAuthTokenExpiredError < Exception
      def initialize(username : String)
        super("OAuth token expired for user '#{username}'")
      end
    end

    class OAuthUser < User
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      @expires_at : Time

      def initialize(@name, @tags, @permissions, @expires_at)
      end

      def expiration=(time : Time)
        @expires_at = time
      end

      def expired? : Bool
        Time.utc > @expires_at
      end

      def can_write?(vhost : String, name : String) : Bool
        raise OAuthTokenExpiredError.new(@name) if expired?
        super
      end

      def can_read?(vhost : String, name : String) : Bool
        raise OAuthTokenExpiredError.new(@name) if expired?
        super
      end

      def can_config?(vhost : String, name : String) : Bool
        raise OAuthTokenExpiredError.new(@name) if expired?
        super
      end

      def can_impersonate? : Bool
        raise OAuthTokenExpiredError.new(@name) if expired?
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
