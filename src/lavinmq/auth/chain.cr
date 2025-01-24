require "./authenticator"
require "./authenticators/basic"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(users : UserStore)
        @backends = [] of Authenticator
        backends = Config.instance.auth_backends
        if backends.nil? || backends.size == 0
          @backends << BasicAuthenticator.new(users)
        else
          backends.each do |backend|
            case backend
            when "basic"
              @backends << BasicAuthenticator.new(users)
            else
              raise "Unsupported authentication backend: #{backend}"
            end
          end
        end
      end

      def authenticate(username : String, password : String)
        @backends.each do |backend|
          if user = backend.authenticate(username, password)
            return user
          end
        end
        nil
      end
    end
  end
end
