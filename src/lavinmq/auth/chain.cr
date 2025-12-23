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
        new(authenticators)
      end

      def authenticate(context : Context) : User?
        @backends.find_value do |backend|
          backend.authenticate(context)
        end
      end
    end
  end
end
