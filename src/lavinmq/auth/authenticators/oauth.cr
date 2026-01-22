require "../authenticator"
require "../oauth_user"
require "../jwt/token_verifier"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      def initialize(@token_verifier : JWT::TokenVerifier)
      end

      def authenticate(context : Context) : BaseUser?
        claims = @token_verifier.verify_token(String.new(context.password))
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, @token_verifier)
      rescue ex : JWT::PasswordFormatError
        Log.debug { "skipping authentication for user \"#{context.username}\": " \
                    "password is not a JWT token" }
      rescue ex : JWT::DecodeError
        Log.debug { "authentication failed for user \"#{context.username}\": " \
                    "Could not decode token - #{ex.message}" }
      rescue ex : JWT::VerificationError
        Log.debug { "authentication failed for user \"#{context.username}\": " \
                    "Token verification failed - #{ex.message}" }
      rescue ex : Exception
        Log.error(exception: ex) { "authentication failed for user \"#{context.username}\": #{ex.message}" }
      end
    end
  end
end
