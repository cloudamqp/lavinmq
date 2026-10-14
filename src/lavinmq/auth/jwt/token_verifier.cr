require "./../../config"
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

        # Everything a verification depends on. A reload swaps in a new State
        # as one reference, and a verification reads @state once, so each token
        # is checked entirely against either the old or the new config, even
        # when ensure_keys blocks while a reload runs. Config#reload overwrites
        # the live Config in place, so each State holds a detached copy of it.
        private record State, config : Config, parser : TokenParser,
          fetcher : JWKSFetcher, expected_issuer : String do
          def self.new(config : Config, fetcher : JWKSFetcher, expected_issuer : String)
            config = config.dup
            new(config, TokenParser.new(config), fetcher, expected_issuer)
          end
        end

        @disabled = false

        def initialize(config : Config, fetcher : JWKSFetcher)
          @state = State.new(config, fetcher, config.oauth_issuer_url.to_s.chomp("/"))
        end

        def fetcher : JWKSFetcher
          @state.fetcher
        end

        # Applies a reloaded config. Claim, scope and audience settings are read
        # from the config on every verification, but the issuer and JWKS cache
        # TTL are bound to the fetcher, so it's replaced when either changes.
        # The verifier itself is kept, as OAuthUsers hold it for UpdateSecret.
        def reload(config : Config) : Nil
          state = @state
          uri = config.oauth_issuer_url
          unless uri
            Log.warn { "oauth issuer removed from the config, keeping the current issuer" }
            @state = State.new(config, state.fetcher, state.expected_issuer)
            return
          end
          issuer = uri.to_s.chomp("/")
          if issuer == state.fetcher.issuer_url && config.oauth_jwks_cache_ttl == state.fetcher.default_cache_ttl
            @state = State.new(config, state.fetcher, state.expected_issuer)
            return
          end
          Log.info { "OAuth issuer or jwks_cache_ttl changed, refetching JWKS from #{issuer}" }
          fetcher = JWKSFetcher.new(uri, config.oauth_jwks_cache_ttl)
          fetcher.start_refresh_loop
          @state = State.new(config, fetcher, issuer)
          state.fetcher.cleanup
        end

        def parse_token(token : String) : TokenClaim
          state = @state
          state.parser.parse(verify_token(token, state))
        end

        # Stops the JWKS fetcher and rejects all tokens from now on, e.g. token
        # refreshes from connections authenticated before OAuth was disabled.
        def disable : Nil
          @disabled = true
          @state.fetcher.cleanup
        end

        def verify_token(token : String) : Token
          verify_token(token, @state)
        end

        private def verify_token(token : String, state : State) : Token
          raise JWT::VerificationError.new("OAuth authentication is disabled") if @disabled
          prevalidate_token(token)
          # After prevalidation so malformed tokens can't trigger network fetches
          state.fetcher.ensure_keys
          verified_token = state.fetcher.public_keys.decode(token)
          payload = verified_token.payload
          validate_issuer(payload, state.expected_issuer)
          validate_audience(payload, state.config) if state.config.oauth_verify_aud?
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

        protected def validate_issuer(payload : JWT::Payload, expected_issuer = @state.expected_issuer)
          issuer = payload.iss
          raise JWT::DecodeError.new("Missing or invalid iss claim in token") unless issuer

          if issuer.chomp("/") != expected_issuer
            raise JWT::VerificationError.new("Token issuer does not match the expected issuer")
          end
        end

        private def validate_audience(payload : JWT::Payload, config = @state.config)
          aud = payload.aud
          raise JWT::VerificationError.new("Missing aud claim in token") unless aud

          audiences = payload.audiences
          raise JWT::DecodeError.new("Invalid aud claim format") unless audiences

          expected = config.oauth_audience || config.oauth_resource_server_id

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
