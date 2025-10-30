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
        # Always try Basic Auth first
        authenticators << BasicAuthenticator.new(users)

        backends = config.auth_backends
        if backends && !backends.empty?
          backends.each do |backend|
            case backend
            when "basic"
              next
            when "oauth"
              authenticators << OAuthAuthenticator.new(users)
            else
              raise "Unsupported authentication backend: #{backend}"
            end
          end
        end
        self.new(authenticators)
      end

      def authenticate(username : String, password : Bytes) : User?
        @backends.find_value do |backend|
          backend.authenticate(username, password)
        end
      end
    end
  end
end
