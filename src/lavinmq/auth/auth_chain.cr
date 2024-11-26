require "./services/auth_service"
require "./services/local_auth_service"
require "./services/http_service"

module LavinMQ
  class AuthenticationChain
    @first_service : AuthenticationService?

    def initialize(@auth_cache = Cache(String, Bool).new(10.seconds))
      @first_service = nil
    end

    def add_service(service : AuthenticationService)
      if first = @first_service
        current = first
        while next_service = current.next_service
          current = next_service
        end
        current.then(service)
      else
        @first_service = service
      end
      self
    end

    def authorize?(username : String, password : String)
      if value = @auth_cache.get?(username)
        return value
      end
      if service = @first_service
        @auth_cache.set(username, service.authorize?(username, password))
      end
    end
  end
end
