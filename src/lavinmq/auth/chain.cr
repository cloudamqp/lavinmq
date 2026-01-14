require "./authenticator"
require "./local_authenticator"
require "./oauth_authenticator"

module LavinMQ
  module Auth
    class Chain < Authenticator
      Log = LavinMQ::Log.for "auth.chain"
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(config : Config, users : UserStore) : Chain
        authenticators = [] of Authenticator
        config.auth_backends.each do |backend|
          case backend
          when "local"
            authenticators << LocalAuthenticator.new(users)
          when "oauth"
            jwks_fetcher = JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
            spawn jwks_fetcher.refresh_loop, name: "JWKS refresh"
            verifier = TokenVerifier.new(config, jwks_fetcher)
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

      def authenticate(username : String, password : String) : BaseUser?
        @backends.find_value do |backend|
          backend.authenticate(username, password)
        end
      end
    end
  end
end
