require "./authenticator"
require "./oauth_user"
require "./../config"
require "jwt"

module LavinMQ
  module Auth
    record TokenClaims,
      username : String,
      tags : Array(Tag),
      permissions : Hash(String, User::Permissions),
      expires_at : Time

    class OAuthAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2"

      # send in the JWKS fetcher here?
      def initialize(@config : Config)
        @expected_issuer = @config.oauth_issuer_url.chomp("/")
      end

      # Todo: Keep verifyer class, so we dont have to have authenticator in User
      def authenticate(username : String, password : String) : OAuthUser?
        claims = verify_token(password)
        OAuthUser.new(claims.username, claims.tags, claims.permissions, claims.expires_at, self)
      rescue ex : JWT::PasswordFormatError
        Log.debug { "skipping authentication for user \"#{username}\": " \
                    "password is not a JWT token" }
        nil
      rescue ex : JWT::DecodeError
        Log.debug { "authentication failed for user \"#{username}\": " \
                    "Could not decode token - #{ex.message}" }
        nil
      rescue ex : JWT::VerificationError
        Log.debug { "authentication failed for user \"#{username}\": " \
                    "Token verification failed - #{ex.message}" }
        nil
      rescue ex : Exception
        Log.error(exception: ex) { "authentication failed for user \"#{username}\": #{ex.message}" }
        nil
      end

      def verify_token(token : String) : TokenClaims
        public_keys = Auth.jwks_fetcher.try(&.public_keys) || raise JWT::VerificationError.new("JWKS fetcher not initialized")

        expected_audience = @config.oauth_verify_aud? ? determine_expected_audience : nil

        verifier_config = JWT::VerifierConfig.new(
          expected_issuer: @expected_issuer,
          expected_audience: expected_audience,
          verify_audience: @config.oauth_verify_aud?,
          time_tolerance: 5.seconds, # To make up for server lag and using RoughTime
          time_source: -> { RoughTime.utc }
        )

        verifier = JWT::Verifier.new(verifier_config, public_keys)
        verified_token = verifier.verify(token)

        validate_audience_strict(verified_token.payload) if @config.oauth_verify_aud?

        username = extract_username(verified_token.payload)
        tags, permissions = parse_roles(verified_token.payload)
        expires_at = verified_token.payload["exp"].as_i64

        TokenClaims.new(username, tags, permissions, Time.unix(expires_at))
      end

      private def determine_expected_audience : String
        @config.oauth_audience.empty? ? @config.oauth_resource_server_id : @config.oauth_audience
      end

      private def validate_audience_strict(payload)
        aud = payload["aud"]?
        return unless aud

        expected = determine_expected_audience

        if expected.empty?
          raise JWT::DecodeError.new("Token contains audience claim but no expected audience is configured")
        end
      end

      private def extract_username(payload) : String
        @config.oauth_preferred_username_claims.each do |claim|
          if username = payload[claim]?.try(&.as_s?)
            return username
          end
        end
        raise JWT::DecodeError.new("No username found in JWT claims (tried: #{@config.oauth_preferred_username_claims.join(", ")})")
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
