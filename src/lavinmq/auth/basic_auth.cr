require "./auth_handler"
require "../server"

module LavinMQ
  class BasicAuthHandler < LavinMQ::AuthHandler

    def authenticate(username : String, password : String, users : UserStore, remote_address : Socket::Address)
      user = users[username]
      # TODO: do not do authentication check if the user is not in the userstore, instead pass directly to the next handler
      return user if user && user.password && user.password.not_nil!.verify(password) &&
                       guest_only_loopback?(remote_address, user)
      puts "Basic authentication failed"
      @successor ? @successor.try &.authenticate(username, password) : nil
    end

    private def guest_only_loopback?(remote_address, user) : Bool
        return true unless user.name == "guest"
        return true unless Config.instance.guest_only_loopback?
        remote_address.loopback?
    end
  end
end
