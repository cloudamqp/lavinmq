require "../authenticator"
require "../oauth_user"
require "../jwt/token_verifier"
require "../jwt/token_claim"

module LavinMQ
  module Auth
    # Authenticates JWTs from one or more OAuth providers. Each token is
    # verified by the provider whose issuer matches its iss claim.
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      # Verifiers by provider name, in config order. Replaced, never mutated,
      # so an authentication in progress keeps the hash it started with.
      @verifiers : Hash(String, JWT::TokenVerifier)

      def initialize(verifiers : Hash(String, JWT::TokenVerifier))
        raise ArgumentError.new("OAuthAuthenticator needs at least one verifier") if verifiers.empty?
        @verifiers = verifiers
      end

      def initialize(verifier : JWT::TokenVerifier)
        @verifiers = {OAuthProviderConfig::DEFAULT_NAME => verifier.as(JWT::TokenVerifier)}
      end

      # Builds a verifier per provider and starts fetching their keys
      def self.create(providers : Array(OAuthProviderConfig)) : self
        new(providers.to_h { |provider| {provider.name, start_verifier(provider)} })
      end

      def self.start_verifier(provider : OAuthProviderConfig) : JWT::TokenVerifier
        fetcher = JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
        verifier = JWT::TokenVerifier.new(provider, fetcher)
        fetcher.start_refresh_loop
        verifier
      end

      # The first provider's verifier
      def token_verifier : JWT::TokenVerifier
        @verifiers.first_value
      end

      # The verifier of the provider management UI SSO logs in with: the first
      # provider where SSO is configured, nil if its SSO settings are invalid
      def sso_verifier : JWT::TokenVerifier?
        @verifiers.each_value do |verifier|
          next unless verifier.provider.sso_configured?
          return verifier if verifier.provider.sso_enabled?
          return
        end
      end

      def verifiers : Array(JWT::TokenVerifier)
        @verifiers.values
      end

      def authenticate(context : Context) : BaseUser?
        token = String.new(context.password)
        verifier = verifier_for(token)
        claims = verifier.parse_token(token)
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, verifier)
      rescue JWT::PasswordFormatError
        Log.debug do
          "skipping authentication for user \"#{context.username}\": " \
          "password is not a JWT token"
        end
      rescue ex : JWT::DecodeError
        Log.debug do
          "authentication failed for user \"#{context.username}\": " \
          "Could not decode token - #{ex.message}"
        end
      rescue ex : JWT::VerificationError
        Log.debug do
          "authentication failed for user \"#{context.username}\": " \
          "Token verification failed - #{ex.message}"
        end
      rescue ex : Exception
        Log.error(exception: ex) { "authentication failed for user \"#{context.username}\": #{ex.message}" }
      end

      private def verifier_for(token : String) : JWT::TokenVerifier
        issuer = JWT::TokenVerifier.unverified_issuer(token)
        @verifiers.each_value do |verifier|
          return verifier if verifier.issuer == issuer
        end
        raise JWT::VerificationError.new("No OAuth provider is configured for the token issuer")
      end

      # Applies the reloaded providers by name: existing providers with an
      # unchanged issuer are reloaded, others started, and the verifiers no
      # longer used disabled, which also rejects token refreshes from the
      # connections they authenticated. Without any provider in the config (no
      # issuer) the current ones are kept.
      def reload(config : Config) : Nil
        providers = config.oauth_providers
        if providers.empty?
          Log.warn { "No OAuth issuer in the config, keeping the current OAuth providers" }
          return
        end
        current = @verifiers
        verifiers = providers.to_h do |provider|
          verifier = current[provider.name]?
          if verifier && verifier.issuer == provider.issuer_url.to_s.chomp("/")
            verifier.reload(provider)
          else
            Log.info { "Starting OAuth provider '#{provider.name}' with issuer #{provider.issuer_url}" }
            verifier = self.class.start_verifier(provider)
          end
          {provider.name, verifier}
        end
        @verifiers = verifiers
        current.each do |name, verifier|
          next if verifiers[name]?.try &.same?(verifier)
          Log.info { "Stopping OAuth provider '#{name}' with issuer #{verifier.issuer}" }
          verifier.disable
        end
      end

      def cleanup
        @verifiers.each_value &.disable
      end
    end
  end
end
