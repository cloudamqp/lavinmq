require "../auth_handler"
require "../../server"

module LavinMQ
  class BasicAuthHandler < LavinMQ::AuthHandler
    def initialize(@users : UserStore)
    end

    def authenticate(username : String, password : String)
      user = @users[username]
      return user if user && user.password && user.password.not_nil!.verify(password)
      @log.warn { "Basic authentication failed" }
      try_next(username, password)
    end
  end
end
