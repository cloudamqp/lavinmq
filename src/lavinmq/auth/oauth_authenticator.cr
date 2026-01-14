require "./authenticator"
require "./oauth_user"
require "./token_verifier"
require "jwt"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      def initialize(@token_verifier : TokenVerifier)
      end

      def authenticate(username : String, password : String) : OAuthUser?
        claims = @token_verifier.verify_token(password)
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, @token_verifier)
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
