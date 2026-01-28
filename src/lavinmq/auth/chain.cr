require "./authenticator"
require "./authenticators/local"
require "./authenticators/oauth"
require "./jwt/public_keys"
require "./jwt/jwks_fetcher"
require "./jwt/token_verifier"

module LavinMQ
  module Auth
    class Chain < Authenticator
      Log = LavinMQ::Log.for "auth.chain"
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(backends : Array(String), users : UserStore, verifier : JWT::TokenVerifier) : Chain
        authenticators = [] of Authenticator
        backends.each do |backend|
          case backend
          when "local"
            authenticators << LocalAuthenticator.new(users)
          when "oauth"
            verifier.fetcher.start_refresh_loop
            authenticators << OAuthAuthenticator.new(verifier)
          else
            raise "Unsupported authentication backend: #{backend}"
          end
        end
        if authenticators.empty?
          # Default to local auth if no backends configured
          authenticators << LocalAuthenticator.new(users)
        end
        self.new(authenticators)
      end

      def authenticate(context : Context) : BaseUser?
        @backends.find_value do |backend|
          backend.authenticate(context)
        end
      end
    end
  end
end
