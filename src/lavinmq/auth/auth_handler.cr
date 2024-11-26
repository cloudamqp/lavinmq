module LavinMQ
  abstract class AuthHandler

    @successor : AuthHandler?

    def initialize(successor : AuthHandler? = nil)
      @successor = successor
    end

    def authenticate(username : String, password : String)
    end
  end
end
