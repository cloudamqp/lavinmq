require "./authenticator"
require "./oauth_user"
require "./../config"
require "./jwt_token_verifier"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"
      @verifier : JWTTokenVerifier

      def initialize(config = Config.instance)
        jwks_fetcher = JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
        @verifier = JWTTokenVerifier.new(config, jwks_fetcher)
      end

      def authenticate(username : String, password : String) : OAuthUser?
        claims = @verifier.verify_token(password)

        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at,
          @verifier)
      rescue ex : JWT::PasswordFormatError
        Log.debug { "skipping authentication for user \"#{username}\": " \
                    "password is not a JWT token" }
        nil
      rescue ex : JWT::DecodeError
        Log.debug { "authentication failed for user \"#{username}\": " \
                    "Could not decode token - #{ex.message}" }
        nil
      rescue ex : JWT::VerificationError
        Log.debug { "authentication failed for user \"#{username}\": " \
                    "Token verification failed - #{ex.message}" }
        nil
      rescue ex : Exception
        Log.error(exception: ex) { "authentication failed for user \"#{username}\": #{ex.message}" }
        nil
      end
    end
  end
end
