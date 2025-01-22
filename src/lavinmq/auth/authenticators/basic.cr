require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class BasicAuthenticator < LavinMQ::Auth::Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : String)
        user = @users[username]
        return user if user && user.password && user.password.not_nil!.verify(password)
        @log.info { "Basic authentication failed" }
        try_next(username, password)
      end
    end
  end
end
