require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class LocalAuthenticator < Authenticator
      def initialize(@users : Auth::UserStore)
      end

      def authenticate(username : String, password : Bytes) : User?
        if user = @users[username]?
          if passwd = user.password
            if passwd.verify(password)
              return user
            end
          end
        end
      rescue ex : Exception
        Log.error { "Local authentication failed: #{ex.message}" }
      end
    end
  end
end
