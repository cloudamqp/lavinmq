require "./authenticator"
require "./local_authenticator"
require "./oauth_authenticator"
require "./public_keys"
require "./jwks_fetcher"
require "./jwt_token_verifier"

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
            authenticators << create_oauth_authenticator(config)
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

      private def self.create_oauth_authenticator(config : Config) : OAuthAuthenticator
        public_keys = PublicKeys.new
        jwks_fetcher = JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)

        # Spawn JWKS refresh fiber
        spawn jwks_refresh_loop(jwks_fetcher, public_keys), name: "JWKS refresh"

        verifier = JWTTokenVerifier.new(config, public_keys)
        OAuthAuthenticator.new(verifier)
      end

      private def self.jwks_refresh_loop(fetcher : JWKSFetcher, public_keys : PublicKeys)
        retry_delay = 5.seconds
        max_retry_delay = 5.minutes

        loop do
          begin
            result = fetcher.fetch_jwks
            public_keys.update(result.keys, result.ttl)
            Log.info { "Refreshed JWKS with #{result.keys.size} key(s), TTL=#{result.ttl}" }
            retry_delay = 5.seconds # Reset backoff on success

            if expires_at = public_keys.expires_at
              sleep_time = expires_at - RoughTime.utc
              sleep sleep_time if sleep_time > 0.seconds
            else
              sleep 60.seconds # Fallback if no expiration
            end
          rescue ex
            Log.error(exception: ex) { "Failed to fetch JWKS: #{ex.message}, retrying in #{retry_delay}" }
            sleep retry_delay
            retry_delay = {retry_delay * 2, max_retry_delay}.min # Exponential backoff
          end
        end
      end

      def authenticate(username : String, password : String) : BaseUser?
        @backends.find_value do |backend|
          backend.authenticate(username, password)
        end
      end
    end
  end
end
