require "../authenticator"
require "../users/temp_user"
require "../../server"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      def initialize(@users : Auth::UserStore)
      end

      def authenticate(username : String, password : Bytes) : Users::TempUser?

        #Todo: send in JWT token and verify it,
        # parse that body and return a TempUser

        if user = @users[username]? # instead: if token is valid
          temp_user = Users::TempUser.new
          temp_user.set_expiration(Time.utc + 3600.milliseconds) # Set expiration to 1 hour
          return temp_user
        end

      rescue ex : Exception
        Log.error { "Oauth authentication failed: #{ex.message}" }
      end
    end
  end
end
