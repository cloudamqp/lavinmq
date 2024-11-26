require "./auth_service"

module LavinMQ
  class LocalAuthService < AuthenticationService
    def authenticate(username : String, password : String, users : UserStore, remote_address : Socket::Address)
      if user = users[username]?
        if user && user.password && user.password.not_nil!.verify(password) &&
                     guest_only_loopback?(remote_address, user)
          return user
        else
          try_next(username, password)
        end
      else
        try_next(username, password)
      end
    end

    private def guest_only_loopback?(remote_address, user) : Bool
      return true unless user.name == "guest"
      return true unless Config.instance.guest_only_loopback?
      remote_address.loopback?
    end
  end
end
