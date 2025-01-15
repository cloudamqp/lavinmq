require "../auth_handler"
require "../../server"

module LavinMQ
  class BasicAuthHandler < LavinMQ::AuthHandler

    def initialize(@users : UserStore)
    end

    def authenticate(username : String, password : String)
      user = @users[username]
      # TODO: do not do authentication check if the user is not in the userstore, instead pass directly to the next handler
      return user if user && user.password && user.password.not_nil!.verify(password)
      puts "Basic authentication failed"
      try_next(username, password)
    end
  end
end
