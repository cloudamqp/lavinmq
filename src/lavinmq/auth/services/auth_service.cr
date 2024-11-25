module LavinMQ
  abstract class AuthenticationService
    property next_service : AuthenticationService?

    abstract def authorize?(username : String, password : String)

    def then(service : AuthenticationService) : AuthenticationService
      @next_service = service
      service
    end

    protected def try_next(username : String, password : String)
      if next_service = @next_service
        next_service.authorize?(username, password)
      end
    end
  end
end
