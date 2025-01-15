module LavinMQ
  abstract class AuthHandler
    property successor : AuthHandler?

    abstract def authenticate(username : String, password : String)

    def set_successor(service : AuthHandler) : AuthHandler
      @successor = service
      service
    end

    def try_next(username : String, password : String)
      if successor = @successor
        successor.authenticate(username, password)
      else
        nil
      end
    end
  end
end
