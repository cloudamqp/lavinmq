require "./authenticator"
require "./oauth_user"
require "./../config"
require "./jwt"
require "./public_keys"
require "./jwks_fetcher"
require "openssl"
require "http/client"
require "./lib_crypto_ext"

module LavinMQ
  module Auth
    module TokenVerifier
      record TokenClaims,
        username : String,
        tags : Array(Tag),
        permissions : Hash(String, User::Permissions),
        expires_at : Time

      abstract def verify_token(token : String) : TokenClaims
    end

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
    # 5. Create OAuthUser: With extracted username, tags, permissions, and expiration time
    #
    # Token refresh: Clients send UpdateSecret frame with new JWT to update permissions
    # without reconnecting. Username stays the same, only permissions/expiration are updated.
    #
    # Public Key caching: Public keys are fetched on first use and cached with TTL from the
    # JWKS endpoints Cache-Control header (or config default). Keys are re-fetched when
    # cache expires.
    #
    # Security: Only RS256 accepted (prevents algorithm confusion), issuer/audience
    # validation prevents accepting tokens from untrusted or unintended sources, fails
    # closed on any error.
    class OAuthAuthenticator < Authenticator
      include TokenVerifier
      Log = LavinMQ::Log.for "oauth2"

      @public_keys = PublicKeys.new
      @jwks_fetcher : JWKSFetcher

      def initialize(@config = Config.instance)
        @jwks_fetcher = JWKSFetcher.new(@config.oauth_issuer_url, @config.oauth_jwks_cache_ttl)
      end

      def authenticate(username : String, password : String) : OAuthUser?
        claims = verify_token(password)

        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, self)
      rescue ex : JWT::PasswordFormatError
        Log.debug { "OAuth2 skipping authentication for user \"#{username}\": password is not a JWT token" }
        nil
      rescue ex : JWT::DecodeError
        Log.debug { "OAuth2 authentication failed for user \"#{username}\": Could not decode token - #{ex.message}" }
        nil
      rescue ex : JWT::VerificationError
        Log.debug { "OAuth2 authentication failed for user \"#{username}\": Token verification failed - #{ex.message}" }
        nil
      rescue ex : Exception
        Log.error(exception: ex) { "OAuth2 authentication failed for user \"#{username}\": #{ex.message}" }
        nil
      end

      # Also used by OAuthUser on UpdateSecret frame
      def verify_token(token : String) : TokenClaims
        prevalidate_token(token)
        verified_token = verify_with_public_key(token)
        validate_and_extract_claims(verified_token.payload)
      end

      private def prevalidate_token(token : String)
        # JWT tokens always start with "ey" (base64 encoded JSON header)
        raise JWT::PasswordFormatError.new unless token.starts_with?("ey")

        parts = token.split('.', 4)
        raise JWT::DecodeError.new("Invalid JWT format") unless parts.size == 3

        header = JWT::RS256Parser.decode_header(token)
        alg = header["alg"]?.try(&.as_s)
        raise JWT::DecodeError.new("Missing algorithm in header") unless alg
        raise JWT::DecodeError.new("Expected RS256, got #{alg}") unless alg == "RS256"

        payload_str = JWT::RS256Parser.base64url_decode(parts[1])
        payload = JSON.parse(payload_str)
        exp = payload["exp"]?.try(&.as_i64?)
        raise JWT::DecodeError.new("Missing exp claim in token") unless exp
        raise JWT::DecodeError.new("Token has expired") if Time.unix(exp) <= RoughTime.utc
      end

      private def verify_with_public_key(token : String) : JWT::Token
        @public_keys.decode(token)
      rescue JWT::DecodeError | JWT::VerificationError
        # Cache miss, expired, or key rotation: fetch fresh keys and retry once
        Log.debug { "JWKS decode failed, fetching fresh keys from issuer" }
        result = @jwks_fetcher.fetch_jwks
        @public_keys.update(result.keys, result.ttl)
        Log.debug { "Updated public_key cache with #{result.keys.size} key(s), TTL=#{result.ttl}" }
        @public_keys.decode(token)
      end

      protected def validate_and_extract_claims(payload) : TokenClaims
        validate_issuer(payload)
        validate_audience(payload) if @config.oauth_verify_aud?

        username = extract_username(payload)
        tags, permissions = parse_roles(payload)
        expires_at = payload["exp"]?.try(&.as_i64?)
        raise JWT::DecodeError.new("No expiration time found in JWT token") unless expires_at
        TokenClaims.new(username, tags, permissions, Time.unix(expires_at))
      end

      protected def validate_issuer(payload)
        issuer = payload["iss"]?.try(&.as_s?)
        raise JWT::DecodeError.new("Missing or invalid iss claim in token") unless issuer

        expected = @config.oauth_issuer_url.chomp("/")
        actual = issuer.chomp("/")

        if actual != expected
          raise JWT::DecodeError.new("Token issuer does not match the expected issuer")
        end
      end

      private def validate_audience(payload)
        aud = payload["aud"]?
        return unless aud

        audiences = case aud
                    when .as_a? then aud.as_a.map(&.as_s)
                    when .as_s? then [aud.as_s]
                    else             return
                    end
        expected = @config.oauth_audience.empty? ? @config.oauth_resource_server_id : @config.oauth_audience
        return if expected.empty?

        if !audiences.includes?(expected)
          raise JWT::DecodeError.new("Token audience does not match expected value")
        end
      end

      private def extract_username(payload) : String
        @config.oauth_preferred_username_claims.each do |claim|
          if username = payload[claim]?.try(&.as_s?)
            return username
          end
        end
        raise "No username found in JWT claims (tried: #{@config.oauth_preferred_username_claims.join(", ")})"
      end

      # Extracts roles/scopes from JWT payload and converts to LavinMQ tags and permissions.
      private def parse_roles(payload)
        tags = Set(Tag).new
        permissions = Hash(String, User::Permissions).new
        scopes = [] of String

        if !@config.oauth_resource_server_id.empty?
          if arr = payload.dig?("resource_access", @config.oauth_resource_server_id, "roles").try(&.as_a?)
            scopes.concat(arr.map(&.as_s))
          end
        end

        if scope_str = payload["scope"]?.try(&.as_s?)
          scopes.concat(scope_str.split)
        end

        if !@config.oauth_additional_scopes_key.empty?
          if claim = payload[@config.oauth_additional_scopes_key]?
            scopes.concat(extract_scopes_from_claim(claim))
          end
        end

        filter_scopes(scopes).each { |scope| parse_role(scope, tags, permissions) }
        {tags.to_a, permissions}
      end

      private def extract_scopes_from_claim(claim) : Array(String)
        case claim
        when .as_h?
          claim.as_h.flat_map do |key, value|
            if @config.oauth_resource_server_id.empty? || key == @config.oauth_resource_server_id
              extract_scopes_from_claim(value)
            end
          end.compact
        when .as_a?
          claim.as_a.flat_map do |item|
            extract_scopes_from_claim(item)
          end
        when .as_s?
          claim.as_s.split
        else
          Array(String).new(0)
        end
      end

      private def filter_scopes(scopes : Array(String)) : Array(String)
        prefix = @config.oauth_scope_prefix
        if prefix.empty? && !@config.oauth_resource_server_id.empty?
          prefix = "#{@config.oauth_resource_server_id}."
        end

        return scopes if prefix.empty?

        scopes.compact_map do |scope|
          scope[prefix.size..] if scope.starts_with?(prefix)
        end
      end

      private def parse_role(role, tags, permissions)
        if role.starts_with?("tag:")
          parse_tag_role(role, tags)
        else
          parse_permission_role(role, permissions)
        end
      end

      private def parse_tag_role(role, tags)
        tag_name = role[4..]
        if tag = Tag.parse?(tag_name)
          tags << tag
        end
      end

      private def parse_permission_role(role, permissions)
        parts = role.split(/[.:\/]/)
        if parts.size != 3
          Log.warn { "Skipping scope '#{role}': Expected format 'permission:vhost:pattern'" }
          return
        end
        perm_type, vhost, pattern = parts[0], parts[1], parts[2]
        return if !perm_type.in?("configure", "read", "write")

        vhost = "/" if vhost == "*"
        pattern = ".*" if pattern == "*"

        permissions[vhost] ||= {
          config: Regex.new("^$"),
          read:   Regex.new("^$"),
          write:  Regex.new("^$"),
        }

        begin
          regex = Regex.new(pattern)
        rescue ex : ArgumentError
          Log.warn { "Skipping scope '#{role}' due to invalid regex pattern: #{ex.message}" }
          return
        end

        permissions[vhost] = case perm_type
                             when "configure" then permissions[vhost].merge({config: regex})
                             when "read"      then permissions[vhost].merge({read: regex})
                             when "write"     then permissions[vhost].merge({write: regex})
                             else                  permissions[vhost]
                             end
      end
    end
  end
end
