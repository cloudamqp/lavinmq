module LavinMQ
  class AuthService
    def initialize(@auth_store : AuthStore, @user_store : UserStore)
    end

    def authenticate(username : String, password : String) : User?
    end
  end
end
