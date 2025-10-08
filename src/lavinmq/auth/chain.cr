require "./authenticator"
require "./authenticators/*"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(config : Config, users : UserStore, temp_users : TempUserStore) : Chain
        backends = config.auth_backends
        authenticators = Array(Authenticator).new
        if backends.nil? || backends.empty?
          authenticators << LocalAuthenticator.new(users)
        else
          backends.each do |backend|
            case backend
            when "local"
              authenticators << LocalAuthenticator.new(users)
            when "oauth"
              authenticators << OAuthAuthenticator.new(temp_users)
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
