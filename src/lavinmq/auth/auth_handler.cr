module LavinMQ
  abstract class AuthHandler
    property successor : AuthHandler?


    # abstract def authenticate?(username : String, password : String) : Bool

    def set_next(service : AuthHandler) : AuthHandler
      @successor = service
      service
    end

    protected def try_next(username : String, password : String)
      if next_service = @next_service
        next_service.authenticate(username, password)
      else
        false
      end
    end
  end
end
