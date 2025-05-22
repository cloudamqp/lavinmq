require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class BasicAuthenticator < Authenticator
      def initialize(@users : UserStore)
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
        Log.error { "Basic authentication failed: #{ex.message}" }
      end
    end
  end
end
