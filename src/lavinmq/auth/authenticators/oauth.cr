require "../authenticator"
require "../users/temp_user"
require "../temp_user_store"
require "../../config"
require "jwt"
require "http/client"

module LavinMQ
  module Auth
    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      def initialize(@temp_users : Auth::TempUserStore)
        @config = Config.instance
      end

      def authenticate(username : String, password : Bytes) : Users::TempUser?
        payload, header = JWT.decode(String.new(password), @config.oauth_public_key, JWT::Algorithm::RS256, verify: true, validate: true)
        username, tags, permissions, expires_at = parse_jwt_payload(payload)
        Log.info { "OAuth2 user authenticated: #{username}" }

        temp_user = Users::TempUser.new(username, tags, permissions, Time.unix(expires_at))
        @temp_users.add(temp_user)
        temp_user
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
        username = nil
        @config.oauth_preferred_username_claims.each do |claim|
          if found_username = payload[claim]?.try(&.as_s?)
            username = found_username
            break
          end
        end
        raise "No username found in JWT claims (tried: #{@config.oauth_preferred_username_claims.join(", ")})" unless username

        tags = Array(Tag).new
        if payload["roles"]?.try(&.as_a?)
          roles = payload["roles"].as_a
          tags << Tag::Administrator if roles.any? { |r| r.as_s? == "admin" }
          tags << Tag::Monitoring if roles.any? { |r| r.as_s? == "monitoring" }
          tags << Tag::Management if roles.any? { |r| r.as_s? == "management" }
        elsif payload["admin"]?.try(&.as_bool?)
          tags << Tag::Administrator
        end

        permissions = Hash(String, Users::TempUser::Permissions).new
        if perms = payload["permissions"]?.try(&.as_h?)
          vhost = perms["vhost"]?.try(&.as_s?) || "/"
          config_pattern = perms["configure"]?.try(&.as_s?) || ".*"
          read_pattern = perms["read"]?.try(&.as_s?) || ".*"
          write_pattern = perms["write"]?.try(&.as_s?) || ".*"

          permissions[vhost] = {
            config: Regex.new(config_pattern),
            read:   Regex.new(read_pattern),
            write:  Regex.new(write_pattern),
          }
        end

        exp = payload["exp"]?.try(&.as_i64?)
        raise "No expiration time found in JWT token" unless exp

        {username, tags, permissions, exp}
      end
    end
  end
end
