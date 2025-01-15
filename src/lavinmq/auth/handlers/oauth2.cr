require "../auth_handler"

module LavinMQ
  class OAuth2Handler < LavinMQ::AuthHandler
    def initialize(@users : UserStore)
    end

    def authenticate(username : String, password : String)
      # TODO: implement the OAuth2 authentication logic and permissions parser here
      if password.starts_with?("oauth")
        puts "OAuth2 authentication successful"
        @users[username]
      else
        puts "OAuth2 authentication failed"
        try_next(username, password)
      end
    end
  end
end
