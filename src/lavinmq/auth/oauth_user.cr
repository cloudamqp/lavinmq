require "./base_user"

module LavinMQ
  module Auth
    class OAuthUser < BaseUser
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      @token_updated = Channel(Nil).new

      def initialize(@name : String, @tags : Array(Tag), @permissions : Hash(String, Permissions),
                     @expires_at : Time, @verifier : JWTTokenVerifier)
      end

      def token_lifetime : Time::Span
        @expires_at - RoughTime.utc
      end

      def on_expiration(&block)
        spawn(name: "OAuthUser#on_expiration") do
          loop do
            select
            when @token_updated.receive
              next
            when timeout(token_lifetime)
              block.call
              break
            end
          end
        rescue Channel::ClosedError
          # Channel closed, exit gracefully
        end
      end

      def close
        @token_updated.close
      end

      def update_secret(new_secret : String)
        claims = @verifier.verify_token(new_secret)

        # Verify the username matches to prevent token substitution attacks
        if claims.username != @name
          raise JWT::VerificationError.new("Token username mismatch: expected '#{@name}', got '#{claims.username}'")
        end

        # Update authorization and expiration (trust new token)
        @tags = claims.tags
        @permissions = claims.permissions
        @expires_at = claims.expires_at
        clear_permissions_cache
        @token_updated.send nil
      end
    end
  end
end
