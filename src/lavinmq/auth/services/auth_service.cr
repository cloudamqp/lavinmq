module LavinMQ
  abstract class AuthenticationService
    property next_service : AuthenticationService?

    abstract def authorize?(username : String, password : String) : Bool

    def then(service : AuthenticationService) : AuthenticationService
      @next_service = service
      service
    end

    protected def try_next(username : String, password : String)
      if next_service = @next_service
        next_service.authorize?(username, password)
      else
        false
      end
    end
  end
end
