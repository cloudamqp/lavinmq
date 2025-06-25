require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : Bytes) : User?
        pp "oauth"
        if user = @users[username]?
          return user
        end
      rescue ex : Exception
        Log.error { "Oauth authentication failed: #{ex.message}" }
      end
    end
  end
end
