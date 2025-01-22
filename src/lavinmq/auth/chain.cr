require "./authenticator"
require "./authenticators/basic"

module LavinMQ
  module Auth
    class Chain
      @first : Authenticator?

      def initialize(users : UserStore)
        backends = Config.instance.auth_backends
        if backends.nil? || backends.size == 0
          add_handler(BasicAuthenticator.new(users))
        else
          backends.each do |backend|
            case backend
            when "basic"
              add_handler(BasicAuthenticator.new(users))
            else
              raise "Unsupported authentication backend: #{backend}"
            end
          end
        end
      end

      def add_handler(auth : Authenticator)
        if first = @first
          current = first
          while successor = current.@successor
            current = successor
          end
          current.set_successor(auth)
        else
          @first = auth
        end
        self
      end

      def authenticate(username : String, password : String)
        @first.try &.authenticate(username, password)
      end
    end
  end
end
