require "../authenticator"
require "../users/temp_user"
require "../../server"

# This file is only used for testing purposes, it is not going to be included in this pull request.

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      def initialize(@users : Auth::UserStore)
      end

      def authenticate(username : String, password : Bytes) : Users::TempUser?
        if user = @users[username]? # instead: if token is valid
          temp_user = Users::TempUser.new(username, user.tags, user.permissions, Time.utc + 5000.milliseconds)
          return temp_user
        end
      rescue ex : Exception
        Log.error { "Oauth authentication failed: #{ex.message}" }
      end
    end
  end
end
