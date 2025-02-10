require "./authenticator"
require "./authenticators/basic"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(config : Config, users : UserStore) : Chain
        backends = config.auth_backends
        authenticators = Array(Authenticator).new
        if backends.nil? || backends.empty?
          authenticators << BasicAuthenticator.new(users)
        else
          backends.each do |backend|
            case backend
            when "basic"
              authenticators << BasicAuthenticator.new(users)
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
