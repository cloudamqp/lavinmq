require "../authenticator"
require "../oauth_user"
require "../jwt/token_verifier"
require "../jwt/token_claim"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      getter token_verifier : JWT::TokenVerifier

      def initialize(@token_verifier : JWT::TokenVerifier)
      end

      def authenticate(context : Context) : BaseUser?
        claims = @token_verifier.parse_token(String.new(context.password))
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, @token_verifier)
      rescue JWT::PasswordFormatError
        Log.debug do
          "skipping authentication for user \"#{context.username}\": " \
          "password is not a JWT token"
        end
      rescue ex : JWT::DecodeError
        Log.debug do
          "authentication failed for user \"#{context.username}\": " \
          "Could not decode token - #{ex.message}"
        end
      rescue ex : JWT::VerificationError
        Log.debug do
          "authentication failed for user \"#{context.username}\": " \
          "Token verification failed - #{ex.message}"
        end
      rescue ex : Exception
        Log.error(exception: ex) { "authentication failed for user \"#{context.username}\": #{ex.message}" }
      end

      def cleanup
        @token_verifier.fetcher.cleanup
      end
    end
  end
end
