module LavinMQ
  class AuthChain
    @first_handler : AuthHandler?

    def initialize(users : UserStore)
      backends = Config.instance.auth_backends
      if backends.empty?
        add_handler(BasicAuthHandler.new(users))
      else
        # need to add initializers to these in order to only send in username & password in auth
        backends.each do |backend|
          case backend
          when "oauth"
            add_handler(OAuth2Handler.new)
          when "http"
            add_handler(HTTPAuthHandler.new)
          when "basic"
            add_handler(BasicAuthHandler.new(users))
          else
            raise "Unsupported authentication backend: #{backend}"
          end
        end
      end
    end

    def add_handler(handler : AuthHandler)
      pp "Adding handler #{handler}"
      if first = @first_handler
        current = first
        while next_handler = current.@successor
          current = next_handler
        end
        current.set_next(handler)
      else
        @first_handler = handler
      end
      self
    end

    def authenticate(username : String, password : String)
      pp "hello #{username} #{password}"
      pp @first_handler

      @first_handler.try &.authenticate(username, password)
    end
  end
end
