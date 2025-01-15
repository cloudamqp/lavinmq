module LavinMQ
  class AuthChain
    @first_handler : AuthHandler?

    def initialize(users : UserStore)
      backends = Config.instance.auth_backends
      if backends.empty?
        add_handler(BasicAuthHandler.new(users))
      else
        # TODO: gather config for http and oauth and send into handlers
        backends.each do |backend|
          case backend
          when "oauth"
            add_handler(OAuth2Handler.new(users))
          when "http"
            add_handler(HTTPAuthHandler.new(users))
          when "basic"
            add_handler(BasicAuthHandler.new(users))
          else
            raise "Unsupported authentication backend: #{backend}"
          end
        end
      end
    end

    def add_handler(handler : AuthHandler)
      if first = @first_handler
        current = first
        while next_handler = current.@successor
          current = next_handler
        end
        current.set_successor(handler)
      else
        @first_handler = handler
      end
      self
    end

    def authenticate(username : String, password : String)
      # TODO: Cache the authorized users, and call authenticate from cache class
      @first_handler.try &.authenticate(username, password)
    end
  end
end
