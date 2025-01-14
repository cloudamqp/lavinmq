module LavinMQ
  abstract class AuthHandler
    @successor : AuthHandler?

    def initialize(successor : AuthHandler? = nil)
      #don't init this here, it is set in then and can be a property.
      @successor = successor
    end

    def then(handler : AuthHandler) : AuthHandler
      @successor = handler
    end

    def authenticate(username : String, password : String)
    end
  end
end
