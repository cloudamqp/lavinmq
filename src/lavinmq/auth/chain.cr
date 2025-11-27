require "./authenticator"
require "./authenticators/*"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(config : Config, users : UserStore) : Chain
        authenticators = Array(Authenticator).new
        # Always try Local Auth first
        authenticators << LocalAuthenticator.new(users)

        backends = config.auth_backends
        if backends && !backends.empty?
          backends.each do |backend|
            case backend
            when "local"
              next
            when "oauth"
              authenticators << OAuthAuthenticator.new
            else
              raise "Unsupported authentication backend: #{backend}"
            end
          end
        end
        self.new(authenticators)
      end

      def authenticate(username : String, password : String) : User?
        @backends.find_value do |backend|
          backend.authenticate(username, password)
        end
      end
    end
  end
end
