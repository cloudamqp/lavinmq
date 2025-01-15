require "http/client"
require "json"
require "../auth_handler"

module LavinMQ
  class HTTPAuthHandler < AuthHandler

    def initialize(@users : UserStore)
    end

    def authenticate(username : String, password : String)
      # TODO: implement the HTTP authentication logic and permissions parser here
      if password.starts_with?("http")
        puts "http authentication successful"
        return @users[username]
      else
        puts "OAuth2 authentication failed"
        return try_next(username, password)
      end
    end
  end
end
