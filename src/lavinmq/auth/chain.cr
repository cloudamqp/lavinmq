require "./authenticator"
require "./authenticators/local"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(users : UserStore) : Chain
        # For now, only LocalAuthenticator is supported
        # When adding more auth backends, LocalAuthenticator should always be tried first
        authenticators = [LocalAuthenticator.new(users)] of Authenticator
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
