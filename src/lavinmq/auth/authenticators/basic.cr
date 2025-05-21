require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class BasicAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : Bytes) : User?
        if user = @users[username]?
          return user if user.password && user.password.not_nil!.verify(password)
        end
      rescue ex : Exception
        Log.error { "Basic authentication failed: #{ex.message}" }
      end
    end
  end
end
