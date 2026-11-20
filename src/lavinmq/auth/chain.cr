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
      property backends : Array(Authenticator)

      def initialize(@backends : Array(Authenticator), @users : UserStore? = nil)
      end

      def self.create(config : Config, users : UserStore) : Chain
        new(build_backends(config, users, [] of Authenticator), users)
      end

      # Rebuilds the backends from auth_backends, reusing (and reloading) the
      # ones still configured. The new list is swapped in with one assignment,
      # so authentications in progress see either the old or the new chain.
      # Removed backends are cleaned up; a removed OAuthAuthenticator rejects
      # token refreshes from connections it already authenticated.
      def reload(config : Config) : Nil
        backends = self.class.build_backends(config, @users, @backends)
        removed = @backends.reject { |b| backends.any? &.same?(b) }
        @backends = backends
        removed.each &.cleanup
      end

      protected def self.build_backends(config : Config, users : UserStore?,
                                        current : Array(Authenticator)) : Array(Authenticator)
        authenticators = [] of Authenticator
        config.auth_backends.each do |backend|
          case backend
          when "local"
            local(users, current).try { |a| authenticators << a }
          when "oauth"
            if existing = current.find(&.is_a?(OAuthAuthenticator))
              existing.reload(config)
              authenticators << existing
            elsif provider = config.oauth_provider
              jwks_fetcher = Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
              verifier = Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
              verifier.fetcher.start_refresh_loop
              authenticators << OAuthAuthenticator.new(verifier)
            else
              Log.error { "oauth authenticator added but missing setting `issuer_url` cannot start" }
            end
          else
            raise "Unsupported authentication backend: #{backend}"
          end
        end
        if authenticators.empty?
          # Default to local auth if no backends configured
          local(users, current).try { |a| authenticators << a }
        end
        authenticators
      end

      private def self.local(users : UserStore?, current : Array(Authenticator)) : Authenticator?
        if existing = current.find(&.is_a?(LocalAuthenticator))
          existing
        elsif users
          LocalAuthenticator.new(users)
        else
          Log.error { "local authenticator added but no user store to authenticate against" }
          nil
        end
      end

      # The OAuth backend currently in the chain, if any
      def oauth_authenticator : OAuthAuthenticator?
        @backends.each do |backend|
          return backend if backend.is_a?(OAuthAuthenticator)
        end
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
