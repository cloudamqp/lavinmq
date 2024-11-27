require "./services/auth_service"
require "./services/local_auth_service"
require "./services/http_service"
require "../cache"
require "../config"

module LavinMQ
  class AuthenticationChain
    @first_service : AuthenticationService?
    @auth_cache : Cache(String, Bool)

    def initialize(@users_store : UserStore, @auth_cache = Cache(String, Bool).new(Config.instance.auth_cache_time))
      @first_service = nil

      backends = Config.instance.auth_backends

      if backends.empty?
        add_service(LocalAuthService.new(@users_store))
      else
        sorted_backends = backends.to_a.sort_by(&.first)

        sorted_backends.each do |(_, backend_type)|
          case backend_type
          when "http"
            # TODO: Verify config before init service
            add_service(HttpAuthService.new(
              Config.instance.auth_http_method.not_nil!,
              Config.instance.auth_http_user_path.not_nil!,
              Config.instance.auth_http_vhost_path.not_nil!,
              Config.instance.auth_http_resource_path.not_nil!,
              Config.instance.auth_http_topic_path.not_nil!
            ))
          when "internal"
            add_service(LocalAuthService.new(@users_store))
          else
            puts "Unknwon backend"
          end
        end

        unless backends.values.includes?("internal")
          add_service(LocalAuthService.new(@users_store))
        end
      end
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
