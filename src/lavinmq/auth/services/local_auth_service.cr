require "./auth_service"

module LavinMQ
  class LocalAuthService < AuthenticationService
    def initialize(@users_store : UserStore)
    end

    def authorize?(username : String, password : String)
      if user = @users_store[username]?
        if user.password && user.password.not_nil!.verify(password)
          "allow"
        else
          try_next(username, password)
        end
      else
        try_next(username, password)
      end
    end
  end
end
