require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class BasicAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : String)
        user = @users[username]
        return user if user && user.password && user.password.not_nil!.verify(password)
        Log.info { "Basic authentication failed" }
      end
    end
  end
end
