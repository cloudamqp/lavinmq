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

      @cached_keys : Hash(String, String)?
      @cache_expires_at : Time?
      @cache_mutex : Mutex = Mutex.new

      def initialize(@config = Config.instance)
      end

      def validate_and_extract_claims(username : String, password : String)
        prevalidate_jwt(password)
        token = fetch_and_verify_jwks(password)
        extracted_username, tags, permissions, expires_at = parse_jwt_payload(token.payload)
        expiration_time = Time.unix(expires_at)
        {extracted_username, tags, permissions, expiration_time}
      end

      def authenticate(username : String, password : Bytes) : OAuthUser?
        password_str = String.new(password)
        extracted_username, tags, permissions, expiration_time = validate_and_extract_claims(username, password_str)
        Log.info { "OAuth2 user authenticated: #{extracted_username}" }

        OAuthUser.new(extracted_username, tags, permissions, expiration_time, self)
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

      private def prevalidate_jwt(token : String)
        parts = token.split('.')
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

      private def fetch_and_verify_jwks(password : String) : JWT::Token
        if cached_keys = get_cached_keys
          return verify_with_keys(password, cached_keys)
        end
        Log.debug { "Cache expired, fetching JWKS from issuer" }
        oidc_config, _ = fetch_url(@config.oauth_issuer_url.chomp("/") + "/.well-known/openid-configuration")
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s?) || raise "Missing jwks_uri in OIDC configuration"
        jwks, headers = fetch_url(jwks_uri)
        keys_array = jwks["keys"]?.try(&.as_a?) || raise "Missing or invalid keys array in JWKS response"

        keys = Hash(String, String).new
        keys_array.each_with_index do |key, idx|
          next unless key["n"]? && key["e"]?
          kid = key["kid"]?.try(&.as_s) || "unknown-#{idx}"
          keys[kid] = to_pem(key["n"].as_s, key["e"].as_s)
        end

        token = verify_with_keys(password, keys)
        update_cache(keys, extract_jwks_ttl(headers))
        token
      end

      private def get_cached_keys : Hash(String, String)?
        @cache_mutex.synchronize do
          return nil if @cached_keys.nil?
          return @cached_keys = nil if @cache_expires_at.try { |exp| Time.utc >= exp }
          @cached_keys.dup
        end
      end

      private def verify_with_keys(password : String, keys : Hash(String, String)) : JWT::Token
        kid = JWT::RS256Parser.decode_header(password)["kid"]?.try(&.as_s) rescue nil
        # If we know the kid matches a key we can avoid iterating through all keys
        if kid && keys[kid]?
          return JWT::RS256Parser.decode(password, keys[kid], verify: true)
        end

        keys.each_value do |key|
          return JWT::RS256Parser.decode(password, key, verify: true)
        rescue JWT::DecodeError | JWT::VerificationError
        end
        raise JWT::VerificationError.new("Could not verify JWT with any key")
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

      private def update_cache(keys : Hash(String, String), ttl : Time::Span)
        @cache_mutex.synchronize do
          @cached_keys = keys
          @cache_expires_at = Time.utc + ttl
          Log.debug { "Updated JWKS cache with #{keys.size} key(s), TTL=#{ttl}" }
        end
      end

      private def extract_jwks_ttl(headers : ::HTTP::Headers) : Time::Span
        if cache_control = headers["Cache-Control"]?
          if match = cache_control.match(/max-age=(\d+)/)
            return match[1].to_i.seconds
          end
        end
        @config.oauth_jwks_cache_ttl
      end

      protected def parse_jwt_payload(payload)
        validate_issuer(payload)
        validate_audience(payload) if @config.oauth_verify_aud?
        username = extract_username(payload)
        tags, permissions = parse_roles(payload)
        exp = payload["exp"]?.try(&.as_i64?) || raise "No expiration time found in JWT token"
        {username, tags, permissions, exp}
      end

      protected def validate_issuer(payload)
        iss = payload["iss"]?.try(&.as_s?)
        raise JWT::DecodeError.new("Missing or invalid iss claim in token") unless iss

        expected = @config.oauth_issuer_url.chomp("/")
        actual = iss.chomp("/")

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
          raise "Token audience does not match expected value"
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
          claim.as_h.compact_map do |key, value|
            if @config.oauth_resource_server_id.empty? || key == @config.oauth_resource_server_id
              extract_scopes_from_claim(value)
            end
          end.flatten
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
