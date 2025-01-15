require "../auth_handler"
require "../../server"

module LavinMQ
  class BasicAuthHandler < LavinMQ::AuthHandler

    def initialize(@users : UserStore)
    end

    def authenticate(username : String, password : String)
      user = @users[username]
      pp "USER: #{user}"
      # TODO: do not do authentication check if the user is not in the userstore, instead pass directly to the next handler
      return user if user && user.password && user.password.not_nil!.verify(password)
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
