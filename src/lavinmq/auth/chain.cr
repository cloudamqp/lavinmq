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

      def self.create(config : Config, users : UserStore) : Chain
        authenticators = [] of Authenticator
        config.auth_backends.each do |backend|
          case backend
          when "local"
            authenticators << LocalAuthenticator.new(users)
          when "oauth"
            if uri = config.oauth_issuer_url
              jwks_fetcher = Auth::JWT::JWKSFetcher.new(uri, config.oauth_jwks_cache_ttl)
              verifier = Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
              verifier.fetcher.start_refresh_loop
              authenticators << OAuthAuthenticator.new(verifier)
            else
              Log.error { "oauth authenticater added but missing setting `issuer_url` cannot start" }
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

      def cleanup
        @backends.each do |backend|
          backend.cleanup
        end
      end

      def authenticate(context : Context) : BaseUser?
        @backends.find_value do |backend|
          backend.authenticate(context)
        end
      end
    end
  end
end
