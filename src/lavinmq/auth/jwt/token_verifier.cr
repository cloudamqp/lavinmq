require "../../config/oauth_provider_config"
require "./jwt"
require "./public_keys"
require "./jwks_fetcher"
require "openssl"
require "http/client"
require "./lib_crypto_ext"
require "./token_claim"

module LavinMQ
  module Auth
    module JWT
      # OAuth 2.0 / OpenID Connect authenticator for LavinMQ.
      #
      # Clients authenticate by providing a JWT token (RS256-signed) as the password field during
      # AMQP/MQTT connection. The username field is used for logging only - authorization is
      # based on permissions/scopes extracted from the JWT token.
      #
      # Verification flow:
      # 1. Prevalidate: Check JWT format, algorithm (RS256), and expiration
      # 2. Verify signature: Decode JWT using OAuth provider's public keys (from JWKS)
      # 3. Validate claims: Check issuer and optionally audience match configuration
      # 4. Extract permissions: Parse scopes/roles and map to LavinMQ permissions
      #
      # Token refresh: Clients send UpdateSecret frame with new JWT to update permissions
      # without reconnecting. Username stays the same, only permissions/expiration are updated.
      #
      # Public Key caching: Public keys are managed by a background fiber that periodically
      # refreshes them based on TTL. Verification uses cached keys, blocking on a
      # rate-limited on-demand fetch only until the first fetch has succeeded.
      #
      # Security: Only RS256 accepted (prevents algorithm confusion), issuer/audience
      # validation prevents accepting tokens from untrusted or unintended sources, fails
      # closed on any error.
      class TokenVerifier
        Log = LavinMQ::Log.for "oauth2"

        # Settings are read once per verification, so each token is checked
        # entirely against either the old or the new provider settings, even
        # when ensure_keys blocks while a reload runs.
        getter provider : OAuthProviderConfig
        getter fetcher : JWKSFetcher
        # The issuer this verifier accepts tokens from, without a trailing "/"
        getter issuer : String
        @disabled = false

        def initialize(@provider : OAuthProviderConfig, @fetcher : JWKSFetcher)
          @issuer = @provider.issuer_url.to_s.chomp("/")
        end

        # The issuer a token claims, read without verifying the token, to pick
        # the verifier for it. The picked verifier checks the issuer again after
        # verifying the signature, so a forged claim only selects which
        # provider's keys the token must be signed with.
        def self.unverified_issuer(token : String) : String
          raise JWT::PasswordFormatError.new unless token.starts_with?("ey")
          parts = token.split('.', 4)
          raise JWT::DecodeError.new("Invalid JWT format") unless parts.size == 3
          payload = JWT::Payload.from_json(JWT::RS256Parser.base64url_decode(parts[1]))
          issuer = payload.iss || raise JWT::DecodeError.new("Missing or invalid iss claim in token")
          issuer.chomp("/")
        rescue ex : JSON::ParseException | Base64::Error
          raise JWT::DecodeError.new("Invalid JWT payload: #{ex.message}")
        end

        # Applies reloaded provider settings with the same issuer. A changed
        # JWKS cache TTL takes effect at the fetcher's next refresh, keeping its
        # keys. OAuthAuthenticator replaces the verifier when the issuer changes.
        def reload(provider : OAuthProviderConfig) : Nil
          unless provider.issuer_url.to_s.chomp("/") == @issuer
            raise ArgumentError.new("Can't reload a verifier for #{@issuer} with issuer #{provider.issuer_url}")
          end
          @fetcher.default_cache_ttl = provider.jwks_cache_ttl
          @provider = provider
        end

        def parse_token(token : String) : TokenClaim
          provider = @provider
          TokenParser.new(provider).parse(verify_token(token, provider))
        end

        # Stops the JWKS fetcher and rejects all tokens from now on, e.g. token
        # refreshes from connections authenticated before OAuth was disabled.
        def disable : Nil
          @disabled = true
          @fetcher.cleanup
        end

        def verify_token(token : String) : Token
          verify_token(token, @provider)
        end

        private def verify_token(token : String, provider : OAuthProviderConfig) : Token
          raise JWT::VerificationError.new("OAuth authentication is disabled") if @disabled
          prevalidate_token(token)
          # After prevalidation so malformed tokens can't trigger network fetches
          @fetcher.ensure_keys
          # ensure_keys can block on a fetch, during which OAuth may be disabled
          raise JWT::VerificationError.new("OAuth authentication is disabled") if @disabled
          verified_token = @fetcher.public_keys.decode(token)
          payload = verified_token.payload
          validate_issuer(payload)
          validate_audience(payload, provider) if provider.verify_aud?
          verified_token
        end

        private def prevalidate_token(token : String)
          # JWT tokens always start with "ey" (base64 encoded JSON header)
          raise JWT::PasswordFormatError.new unless token.starts_with?("ey")

          parts = token.split('.', 4)
          raise JWT::DecodeError.new("Invalid JWT format") unless parts.size == 3

          header = JWT::RS256Parser.decode_header(token)
          raise JWT::DecodeError.new("Expected RS256, got #{header.alg}") unless header.alg == "RS256"

          payload_str = JWT::RS256Parser.base64url_decode(parts[1])
          payload = JWT::Payload.from_json(payload_str)
          exp = payload.exp
          raise JWT::DecodeError.new("Missing exp claim in token") unless exp
          raise JWT::VerificationError.new("Token has expired") if Time.unix(exp) <= RoughTime.utc

          # Validate iat (Issued At) - reject tokens issued in the future
          # Allow 200ms tolerance for clock skew between the issuer and us
          if iat = payload.iat
            raise JWT::DecodeError.new("Token issued in the future") if Time.unix(iat) > RoughTime.utc + 0.2.seconds
          end

          # Validate nbf (Not Before) - RFC 7519 Section 4.1.5:
          # "if the 'nbf' claim is present... the JWT MUST NOT be accepted for processing"
          if nbf = payload.nbf
            raise JWT::DecodeError.new("Token not yet valid") if Time.unix(nbf) > RoughTime.utc
          end
        end

        protected def validate_issuer(payload : JWT::Payload)
          issuer = payload.iss
          raise JWT::DecodeError.new("Missing or invalid iss claim in token") unless issuer

          if issuer.chomp("/") != @issuer
            raise JWT::VerificationError.new("Token issuer does not match the expected issuer")
          end
        end

        private def validate_audience(payload : JWT::Payload, provider = @provider)
          aud = payload.aud
          raise JWT::VerificationError.new("Missing aud claim in token") unless aud

          audiences = payload.audiences
          raise JWT::DecodeError.new("Invalid aud claim format") unless audiences

          expected = provider.audience || provider.resource_server_id

          # RFC 7519 Section 4.1.3: if the token has an audience claim and we have
          # no expected audience configured, reject it
          if expected.nil?
            raise JWT::DecodeError.new("Token contains audience claim but no expected audience is configured")
          end

          unless audiences.includes?(expected)
            raise JWT::VerificationError.new("Token audience does not match expected value")
          end
        end
      end
    end
  end
end
