require "./base_user"

module LavinMQ
  module Auth
    class OAuthUser < BaseUser
      getter name : String
      getter tags : Array(Tag)
      getter permissions : Hash(String, Permissions)

      def initialize(@name : String, @tags : Array(Tag), @permissions : Hash(String, Permissions),
                     @expires_at : Time, @verifier : JWT::TokenVerifier)
      end

      def token_lifetime : Time::Span
        v = @expires_at - RoughTime.utc
        v.negative? ? 0.seconds : v
      end

      def refresh(new_secret : String)
        claims = @verifier.parse_token(new_secret)

        # Verify the username matches to prevent token substitution attacks
        if claims.username != @name
          raise JWT::VerificationError.new("Token username mismatch: expected '#{@name}', got '#{claims.username}'")
        end

        # Update authorization and expiration (trust new token)
        @tags = claims.tags
        @permissions = claims.permissions
        @expires_at = claims.expires_at
        clear_permissions_cache
      end

      def find_permission(vhost : String) : Permissions?
        permissions[vhost]? || permissions.find { |pattern, _| wildcard_match?(pattern, vhost) }.try(&.[1])
      end

      private def wildcard_match?(pattern : String, value : String) : Bool
        return true if pattern == "*"
        return pattern == value unless pattern.includes?('*')

        parts = pattern.split('*')
        pos = 0

        parts.each_with_index do |part, i|
          next if part.empty?

          if i == 0
            # First part must match at the beginning
            return false unless value.starts_with?(part)
            pos = part.size
          elsif i == parts.size - 1
            # Last part must match at the end
            return false unless value.ends_with?(part)
            return false if value.size - part.size < pos
          else
            # Middle parts just need to be found after current position
            idx = value.index(part, pos)
            return false if idx.nil?
            pos = idx + part.size
          end
        end

        true
      end
    end
  end
end
