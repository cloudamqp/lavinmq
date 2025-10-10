require "../authenticator"
require "../temp_user"
require "../user_store"
require "../../config"
require "jwt"
require "openssl"
require "http/client"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      def initialize(@users : Auth::UserStore)
        @config = Config.instance
      end

      def authenticate(username : String, password : Bytes) : TempUser?
        payload, headers = parse_jwks(String.new(password))
        pp payload
        pp headers
        username, tags, permissions, expires_at = parse_jwt_payload(payload)
        Log.info { "OAuth2 user authenticated: #{username}" }

        @users.add(username, tags, permissions, Time.unix(expires_at))
      rescue ex : JWT::ExpiredSignatureError
        Log.warn { "OAuth2 authentication failed: Token expired" }
        nil
      rescue ex : JWT::InvalidIssuerError
        Log.warn { "OAuth2 authentication failed: Invalid issuer - #{ex.message}" }
        nil
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

      private def parse_jwks(password : String)
        oidc_url = @config.oidc_issuer_url.chomp("/") + "/.well-known/openid-configuration"
        oidc_config = fetch_url(oidc_url)
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s)
        raise "No jwks_uri found in OIDC configuration" unless jwks_uri

        Log.debug { "Fetching JWKS from #{jwks_uri}" }
        jwks = fetch_url(jwks_uri)

        jwks["keys"].as_a.each do |key|
          next unless key["n"]? && key["e"]?
          public_key = to_pem(key["n"].as_s, key["e"].as_s)
          return JWT.decode(password, public_key, JWT::Algorithm::RS256, verify: true, validate: true)
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

      private def to_pem(n, e) : String
        modulus = OpenSSL::BN.from_bin(Base64.decode(n))
        exponent = OpenSSL::BN.from_bin(Base64.decode(e))
        rsa = LibCrypto.rsa_new
        io = IO::Memory.new

        LibCrypto.rsa_set0_key(rsa, modulus, exponent, nil)
        bio = OpenSSL::GETS_BIO.new(io) # also works OpenSSL::BIO.new(io)
        LibCrypto.pem_write_bio_rsa_pub_key(bio, rsa)

        io.to_s
      end
    end
  end
end
