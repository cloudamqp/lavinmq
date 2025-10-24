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

      def initialize(@users : Auth::UserStore, @config = Config.instance)
      end

      def authenticate(username : String, password : Bytes) : OAuthUser?
        token = parse_and_verify_jwks(String.new(password))
        username, tags, permissions, expires_at = parse_jwt_payload(token.payload)
        Log.info { "OAuth2 user authenticated: #{username}" }

        OAuthUser.new(username, tags, permissions, Time.unix(expires_at))
      rescue ex : JWT::DecodeError
        Log.warn { "OAuth2 authentication failed: Could not decode token - #{ex.message}" }
        nil
      rescue ex : JWT::VerificationError
        Log.warn { "OAuth2 authentication failed: Token verification failed - #{ex.message}" }
        nil
      rescue ex : Exception
        Log.error { "OAuth2 authentication failed: #{ex.message}" }
        nil
      end

      private def parse_jwt_payload(payload)
        validate_audience(payload) if @config.oauth_verify_aud
        username = extract_username(payload)
        tags, permissions = parse_roles(payload)
        exp = payload["exp"]?.try(&.as_i64?) || raise "No expiration time found in JWT token"
        {username, tags, permissions, exp}
      end

      private def validate_audience(payload)
        aud = payload["aud"]?
        return unless aud

        audiences = case aud
                    when .as_a? then aud.as_a.map(&.as_s)
                    when .as_s? then [aud.as_s]
                    else             return
                    end
        expected = @config.oauth_audience || @config.oauth_resource_server_id
        return unless expected

        unless audiences.includes?(expected)
          raise "Token audience mismatch: expected '#{expected}', got #{audiences.inspect}"
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

        if resource_id = @config.oauth_resource_server_id
          if arr = payload.dig?("resource_access", resource_id, "roles").try(&.as_a?)
            scopes.concat(arr.map(&.as_s))
          end
        end

        if scope_str = payload["scope"]?.try(&.as_s?)
          scopes.concat(scope_str.split)
        end

        if key = @config.oauth_additional_scopes_key
          if claim = payload[key]?
            scopes.concat(extract_scopes_from_claim(claim))
          end
        end

        filter_scopes(scopes).each { |scope| parse_role(scope, tags, permissions) }
        {tags.to_a, permissions}
      end

      private def filter_scopes(scopes : Array(String)) : Array(String)
        prefix = @config.oauth_scope_prefix
        if prefix.nil?
          if resource_id = @config.oauth_resource_server_id
            prefix = "#{resource_id}."
          end
        end

        return scopes if prefix.nil? || prefix.empty?

        scopes.compact_map do |scope|
          scope[prefix.size..] if scope.starts_with?(prefix)
        end
      end

      private def extract_scopes_from_claim(claim) : Array(String)
        case claim
        when .as_h?
          result = [] of String
          claim.as_h.each do |key, value|
            if resource_id = @config.oauth_resource_server_id
              result.concat(extract_scopes_from_claim(value)) if key == resource_id
            else
              result.concat(extract_scopes_from_claim(value))
            end
          end
          result
        when .as_a?
          result = [] of String
          claim.as_a.each do |item|
            result.concat(extract_scopes_from_claim(item))
          end
          result
        when .as_s?
          claim.as_s.split
        else
          [] of String
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def parse_role(role, tags, permissions)
        if role.starts_with?("tag:")
          tag_name = role[4..]
          if tag = Tag.parse?(tag_name)
            tags << tag
           end
          return
        end

        parts = role.split(/[.:\/]/)
        return unless parts.size == 3
        perm_type, vhost, pattern = parts[0], parts[1], parts[2]
        return unless perm_type.in?("configure", "read", "write")

        vhost = "/" if vhost == "*"
        pattern = ".*" if pattern == "*"

        permissions[vhost] ||= {
          config: Regex.new("^$"),
          read:   Regex.new("^$"),
          write:  Regex.new("^$"),
        }

        regex = Regex.new(pattern)
        permissions[vhost] = case perm_type
                             when "configure" then permissions[vhost].merge({config: regex})
                             when "read"      then permissions[vhost].merge({read: regex})
                             when "write"     then permissions[vhost].merge({write: regex})
                             else                  permissions[vhost]
                             end
      end

      private def parse_and_verify_jwks(password : String) : JWT::Token
        issuer_url = @config.oauth_issuer_url
        raise "OAuth issuer URL not configured" unless issuer_url

        discovery_url = issuer_url.chomp("/") + "/.well-known/openid-configuration"
        oidc_config = fetch_url(discovery_url)
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s)
        raise "No jwks_uri found in OIDC configuration" unless jwks_uri

        Log.debug { "Fetching JWKS from #{jwks_uri}" }
        jwks = fetch_url(jwks_uri)

        jwks["keys"].as_a.each do |key|
          next unless key["n"]? && key["e"]?
          public_key_pem = to_pem(key["n"].as_s, key["e"].as_s)
          return JWT::RS256Parser.decode(password, public_key_pem, verify: true)
        rescue JWT::DecodeError | JWT::VerificationError
          next
        end
        raise "Could not verify JWT with any key from JWKS"
      end

      private def fetch_url(url : String, timeout : Time::Span = 10.seconds)
        uri = URI.parse(url)
        ::HTTP::Client.new(uri) do |client|
          client.connect_timeout = timeout
          client.read_timeout = timeout
          response = client.get(uri.request_target)
          unless response.success?
            raise "HTTP request failed with status #{response.status_code}: #{response.body}"
          end
          JSON.parse(response.body)
        end
      end

      private def to_pem(n : String, e : String) : String
        # Decode base64url-encoded modulus and exponent
        n_bytes = base64url_decode(n)
        e_bytes = base64url_decode(e)

        # Convert bytes to BIGNUMs
        modulus = LibCrypto.bn_bin2bn(n_bytes, n_bytes.size, nil)
        exponent = LibCrypto.bn_bin2bn(e_bytes, e_bytes.size, nil)

        # Create RSA structure
        rsa = LibCrypto.rsa_new
        raise "Failed to create RSA structure" if rsa.null?

        # Set the key components (this takes ownership of the BIGNUMs)
        result = LibCrypto.rsa_set0_key(rsa, modulus, exponent, nil)
        unless result == 1
          LibCrypto.rsa_free(rsa)
          raise "Failed to set RSA key components"
        end

        # Create a memory BIO
        bio = LibCrypto.BIO_new(LibCrypto.bio_s_mem)
        raise "Failed to create BIO" if bio.null?

        begin
          # Write the public key to the BIO in PEM format
          result = LibCrypto.pem_write_bio_rsa_pubkey(bio, rsa)
          unless result == 1
            raise "Failed to write PEM"
          end

          # Get the length of data in the BIO (BIO_CTRL_PENDING = 10)
          length = LibCrypto.bio_ctrl(bio, 10, 0, nil)

          # Read the PEM data from the BIO
          buffer = Bytes.new(length)
          LibCrypto.bio_read(bio, buffer, length.to_i32)

          String.new(buffer)
        ensure
          LibCrypto.BIO_free(bio)
          LibCrypto.rsa_free(rsa)
        end
      end

      private def base64url_decode(input : String) : Bytes
        # Add padding if needed
        padded = case input.size % 4
                 when 2 then input + "=="
                 when 3 then input + "="
                 else        input
                 end

        # Replace URL-safe characters
        standard = padded.tr("-_", "+/")
        Base64.decode(standard)
      end
    end
  end
end
