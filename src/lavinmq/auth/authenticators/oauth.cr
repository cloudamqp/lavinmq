require "../authenticator"
require "../users/oauth_user"
require "../../config"
require "../jwt"
require "openssl"
require "http/client"
require "../lib_crypto_ext"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      record TokenClaims,
        username : String,
        tags : Array(Tag),
        permissions : Hash(String, User::Permissions),
        expires_at : Time

      @cached_public_keys : Hash(String, String)?
      @cache_expires_at : Time?
      @cache_mutex = Mutex.new

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
      def initialize(@config = Config.instance)
      end

      def authenticate(username : String, password : String) : OAuthUser?
        claims = verify_token(password)

        Log.info { "OAuth2 user authenticated: #{claims.username}" }
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, self)
      rescue ex : JWT::DecodeError
        Log.warn { "OAuth2 authentication failed for user \"#{username}\": Could not decode token - #{ex.message}" }
        nil
      rescue ex : JWT::VerificationError
        Log.warn { "OAuth2 authentication failed for user \"#{username}\": Token verification failed - #{ex.message}" }
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
        parts = token.split('.', 4)
        raise JWT::DecodeError.new("Invalid JWT format") unless parts.size == 3

        header = JWT::RS256Parser.decode_header(token)
        alg = header["alg"]?.try(&.as_s)
        raise JWT::DecodeError.new("Missing algorithm in header") unless alg
        raise JWT::DecodeError.new("Expected RS256, got #{alg}") unless alg == "RS256"

        payload_str = base64url_decode(parts[1])
        payload = JSON.parse(payload_str)
        exp = payload["exp"]?.try(&.as_i64?)
        raise JWT::DecodeError.new("Missing exp claim in token") unless exp
        raise JWT::DecodeError.new("Token has expired") if Time.unix(exp) <= Time.utc
      end

      private def verify_with_public_key(token : String) : JWT::Token
        if verified_token = with_cached_public_keys { |keys| decode_token(token, keys) }
          return verified_token
        end

        # Cache miss: Construct new public_keys from JWKS, and verify token
        Log.debug { "JWKS cache expired, fetching from issuer" }
        jwks, headers = fetch_jwks
        public_keys = extract_public_keys_from_jwks(jwks)
        ttl = extract_jwks_ttl(headers)
        decode_token(token, public_keys).tap do
          update_cache(public_keys, ttl)
        end
      end

      private def with_cached_public_keys(& : Hash(String, String) -> JWT::Token)
        @cache_mutex.synchronize do
          keys = @cached_public_keys
          return nil if keys.nil?
          return @cached_public_keys = nil if @cache_expires_at.try { |exp| Time.utc >= exp }
          yield keys
        end
      end

      # Decodes and verifies JWT signature using provided JWKS keys.
      private def decode_token(token : String, public_keys : Hash(String, String)) : JWT::Token
        kid = JWT::RS256Parser.decode_header(token)["kid"]?.try(&.as_s) rescue nil
        # If we know the kid matches a key we can avoid iterating through all keys
        if kid && public_keys[kid]?
          return JWT::RS256Parser.decode(token, public_keys[kid], verify: true)
        end

        public_keys.each_value do |key|
          return JWT::RS256Parser.decode(token, key, verify: true)
        rescue JWT::DecodeError | JWT::VerificationError
        end
        raise JWT::VerificationError.new("Could not verify JWT with any key")
      end

      # Fetches JWKS (JSON Web Key Set) from OAuth provider.
      private def fetch_jwks
        # Discover jwks_uri from OIDC configuration
        oidc_config, _ = fetch_url("#{@config.oauth_issuer_url.chomp("/")}/.well-known/openid-configuration")
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s?) || raise "Missing jwks_uri in OIDC configuration"

        fetch_url(jwks_uri)
      end

      # Parses JWKS and converts RSA keys to PEM format.
      private def extract_public_keys_from_jwks(jwks : JSON::Any)
        jwks_array = jwks["keys"]?.try(&.as_a?) || raise "Missing or invalid keys array in JWKS response"

        public_keys = {} of String => String
        jwks_array.each_with_index do |key, idx|
          next unless key["n"]? && key["e"]?
          kid = key["kid"]?.try(&.as_s) || "unknown-#{idx}"
          public_keys[kid] = to_pem(key["n"].as_s, key["e"].as_s)
        end
        public_keys
      end

      private def extract_jwks_ttl(headers) : Time::Span
        if cache_control = headers["Cache-Control"]?
          if match = cache_control.match(/max-age=(\d+)/)
            # JWKS header overrides lavinmq config
            return match[1].to_i.seconds
          end
        end
        @config.oauth_jwks_cache_ttl
      end

      private def update_cache(public_keys : Hash(String, String), ttl : Time::Span)
        @cache_mutex.synchronize do
          @cached_public_keys = public_keys
          @cache_expires_at = Time.utc + ttl
          Log.debug { "Updated public_key cache with #{public_keys.size} key(s), TTL=#{ttl}" }
        end
      end

      protected def validate_and_extract_claims(payload) : TokenClaims
        validate_issuer(payload)
        validate_audience(payload) if @config.oauth_verify_aud?

        username = extract_username(payload)
        tags, permissions = parse_roles(payload)
        expires_at = payload["exp"]?.try(&.as_i64?) || raise JWT::VerificationError.new("No expiration time found in JWT token")
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

      protected def validate_audience(payload)
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
          end.compact!
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
        return if parts.size != 3
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

      private def fetch_url(url : String) : {JSON::Any, ::HTTP::Headers}
        uri = URI.parse(url)
        ::HTTP::Client.new(uri) do |client|
          client.connect_timeout = 5.seconds
          client.read_timeout = 10.seconds
          response = client.get(uri.request_target)
          if !response.success?
            raise "HTTP request failed with status #{response.status_code}: #{response.body}"
          end
          {JSON.parse(response.body), response.headers}
        end
      end

      # Converts JWKS RSA key components (n, e) to PEM format for JWT verification.
      private def to_pem(n : String, e : String) : String
        # Decode base64url-encoded modulus and exponent
        n_bytes = base64url_decode_bytes(n)
        e_bytes = base64url_decode_bytes(e)

        # Convert bytes to BIGNUMs
        modulus = LibCrypto.bn_bin2bn(n_bytes, n_bytes.size, nil)
        raise "Failed to create modulus" if modulus.null?

        exponent = LibCrypto.bn_bin2bn(e_bytes, e_bytes.size, nil)
        if exponent.null?
          LibCrypto.bn_free(modulus)
          raise "Failed to create exponent"
        end

        # Create RSA structure
        rsa = LibCrypto.rsa_new
        if rsa.null?
          LibCrypto.bn_free(modulus)
          LibCrypto.bn_free(exponent)
          raise "Failed to create RSA structure"
        end

        result = LibCrypto.rsa_set0_key(rsa, modulus, exponent, nil)
        if result != 1
          LibCrypto.bn_free(modulus)
          LibCrypto.bn_free(exponent)
          LibCrypto.rsa_free(rsa)
          raise "Failed to set RSA key components"
        end

        # Create a memory BIO
        bio = LibCrypto.BIO_new(LibCrypto.bio_s_mem)
        if bio.null?
          LibCrypto.rsa_free(rsa)
          raise "Failed to create BIO"
        end

        begin
          # Write the public key to the BIO in PEM format
          result = LibCrypto.pem_write_bio_rsa_pubkey(bio, rsa)
          if result != 1
            raise "Failed to write PEM"
          end

          # Get the length of data in the BIO (BIO_CTRL_PENDING = 10)
          length = LibCrypto.bio_ctrl(bio, 10, 0, nil)
          # RSA-16384 (max) is ~4KB in PEM format, 10KB covers all RSA keys with margin
          raise "Suspiciously large PEM length: #{length}" if length > 10_000

          # Read the PEM data from the BIO
          buffer = Bytes.new(length)
          LibCrypto.bio_read(bio, buffer, length.to_i32)

          String.new(buffer)
        ensure
          LibCrypto.BIO_free(bio)
          LibCrypto.rsa_free(rsa)
        end
      end

      private def prepare_base64url(input : String) : String
        # Add padding if needed
        padded = case input.size % 4
                 when 2 then input + "=="
                 when 3 then input + "="
                 else        input
                 end

        # Replace URL-safe characters
        padded.tr("-_", "+/")
      end

      private def base64url_decode(input : String) : String
        Base64.decode_string(prepare_base64url(input))
      end

      private def base64url_decode_bytes(input : String) : Bytes
        Base64.decode(prepare_base64url(input))
      end
    end
  end
end
