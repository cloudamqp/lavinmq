module LavinMQ
  class OAuth2Handler < LavinMQ::AuthHandler
    def authenticate(username : String, password : String)
      # TODO: implement the OAuth2 authentication logic and permissions parser here
      if password.starts_with?("oauth")
        puts "OAuth2 authentication successful"
        return nil
      else
        puts "OAuth2 authentication failed"
        return @successor ? @successor.try &.authenticate(username, password) : nil
      end
    end
  end
end
