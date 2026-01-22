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

      def self.create(config : Config, users : UserStore, jwks_fetcher : JWT::JWKSFetcher) : Chain
        authenticators = [] of Authenticator
        config.auth_backends.each do |backend|
          case backend
          when "local"
            authenticators << LocalAuthenticator.new(users)
          when "oauth"
            begin
              # We want to make sure the Oauth server is alive before creating the authenticator
              jwks_fetcher.fetch_and_update
              spawn jwks_fetcher.refresh_loop, name: "JWKS refresh"
              verifier = JWT::TokenVerifier.new(config, jwks_fetcher)
              authenticators << OAuthAuthenticator.new(verifier)
            rescue ex
              Log.error(exception: ex) { "Oauth Authenticator failed to initialize" }
            end
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
