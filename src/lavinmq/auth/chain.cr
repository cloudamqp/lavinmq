require "./authenticator"
require "./local_authenticator"
require "./oauth_authenticator"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(config : Config, users : UserStore) : Chain
        authenticators = [] of Authenticator

        backends = config.auth_backends
        if backends.empty?
          # Default to local auth if no backends configured
          authenticators << LocalAuthenticator.new(users)
        else
          backends.each do |backend|
            case backend
            when "local"
              authenticators << LocalAuthenticator.new(users)
            when "oauth"
              authenticators << OAuthAuthenticator.new(config)
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
