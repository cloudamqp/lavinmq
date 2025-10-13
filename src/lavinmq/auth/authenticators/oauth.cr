require "../authenticator"
require "../temp_user"
require "../user_store"
require "../../config"
require "../jwt"
require "openssl"
require "http/client"

lib LibCrypto
  alias BIGNUM = Void*
  type RSA = Void*

  fun bn_bin2bn = BN_bin2bn(s : UInt8*, len : Int32, ret : BIGNUM) : BIGNUM
  fun rsa_new = RSA_new : RSA
  fun rsa_free = RSA_free(rsa : RSA)
  fun rsa_set0_key = RSA_set0_key(rsa : RSA, n : BIGNUM, e : BIGNUM, d : BIGNUM) : Int32
  fun pem_write_bio_rsa_pubkey = PEM_write_bio_RSA_PUBKEY(bio : Bio*, rsa : RSA) : Int32
  fun bio_s_mem = BIO_s_mem : BioMethod*
  fun bio_read = BIO_read(bio : Bio*, data : UInt8*, len : Int32) : Int32
  fun bio_ctrl = BIO_ctrl(bio : Bio*, cmd : Int32, larg : LibC::Long, parg : Void*) : LibC::Long
end

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      def initialize(@users : Auth::UserStore)
        @config = Config.instance
      end

      def authenticate(username : String, password : Bytes) : TempUser?
        token = parse_and_verify_jwks(String.new(password))
        pp token.header
        pp token.payload
        username, tags, permissions, expires_at = parse_jwt_payload(token.payload)
        Log.info { "OAuth2 user authenticated: #{username}" }

        @users.add(username, tags, permissions, Time.unix(expires_at))
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
        username = extract_username(payload)
        tags, permissions = parse_roles(payload)
        exp = payload["exp"]?.try(&.as_i64?) || raise "No expiration time found in JWT token"
        {username, tags, permissions, exp}
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
        permissions = Hash(String, TempUser::Permissions).new

        # Extract roles from resource_access
        if resource_id = @config.oauth_resource_server_id
          if arr = payload.dig?("resource_access", resource_id, "roles").try(&.as_a?)
            arr.each { |r| parse_role(r.as_s, tags, permissions) }
          end
        end

        # Extract roles from additional scopes
        if key = @config.oauth_additional_scopes_key
          if scopes = payload[key]?
            if scopes_arr = scopes.as_a?
              scopes_arr.each { |r| parse_role(r.as_s, tags, permissions) }
            elsif scopes_str = scopes.as_s?
              scopes_str.split.each { |r| parse_role(r, tags, permissions) }
            end
          end
        end

        {tags.to_a, permissions}
      end

      private def parse_role(role, tags, permissions)
        if idx = role.index(".tag:")
          tag_name = role.byte_slice(idx + 5)
          tags << Tag::Administrator if tag_name == "administrator"
          tags << Tag::Monitoring if tag_name == "monitoring"
          tags << Tag::Management if tag_name == "management"
        elsif parts = role.split(/[.:\/]/, 4)
          return unless parts.size == 4
          perm_type, vhost, pattern = parts[1], parts[2], parts[3]
          return unless perm_type.in?("configure", "read", "write")

          vhost = "/" if vhost == "*"
          pattern = ".*" if pattern == "*"

          permissions[vhost] ||= {
            config: Regex.new(".*"),
            read:   Regex.new(".*"),
            write:  Regex.new(".*"),
          }

          regex = Regex.new(pattern)
          permissions[vhost] = case perm_type
                               when "configure" then permissions[vhost].merge({config: regex})
                               when "read"      then permissions[vhost].merge({read: regex})
                               when "write"     then permissions[vhost].merge({write: regex})
                               else                  permissions[vhost]
                               end
        end
      end

      private def parse_and_verify_jwks(password : String) : JWT::Token
        oidc_url = @config.oidc_issuer_url.chomp("/") + "/.well-known/openid-configuration"
        oidc_config = fetch_url(oidc_url)
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s)
        raise "No jwks_uri found in OIDC configuration" unless jwks_uri

        Log.debug { "Fetching JWKS from #{jwks_uri}" }
        jwks = fetch_url(jwks_uri)

        jwks["keys"].as_a.each do |key|
          next unless key["n"]? && key["e"]?
          public_key_pem = to_pem(key["n"].as_s, key["e"].as_s)
          pp public_key_pem
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
