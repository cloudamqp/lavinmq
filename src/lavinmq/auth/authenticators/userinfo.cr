require "../authenticator"
require "../users/userinfo_user"
require "../../config"
require "http/client"
require "json"

module LavinMQ
  module Auth
    # Userinfo endpoint authenticator - validates opaque OAuth tokens
    #
    # This authenticator complements the OAuth (JWT) authenticator by supporting
    # OAuth providers that issue opaque tokens (non-JWT) like GitHub personal access
    # tokens. It validates tokens by calling the OAuth provider's userinfo endpoint.
    #
    # Flow:
    # 1. Client connects with opaque token as password
    # 2. Authenticator calls configured userinfo endpoint with token
    # 3. If successful, extracts username and creates UserinfoUser with default permissions
    # 4. User gets full access to configured default vhost
    #
    # Performance Considerations:
    # - HTTP call on EVERY authentication (~100ms per auth)
    # - New HTTP client created per auth (no connection pooling)
    # - Can become bottleneck under high authentication load
    # - Recommended: Use JWT authentication (OAuthAuthenticator) for better performance
    # - Future: Consider implementing token caching to reduce HTTP calls
    #
    # Security:
    # - Tokens are never logged to prevent exposure
    # - Uses 5s connect timeout and 10s read timeout
    # - Validates JSON responses before parsing
    class UserinfoAuthenticator < Authenticator
      Log = LavinMQ::Log.for "oauth2.userinfo"

      def initialize(@config = Config.instance)
      end

      def authenticate(username : String, password : String) : UserinfoUser?
        return nil if @config.oauth_userinfo_url.empty?

        user_info = validate_token(password)
        return nil unless user_info

        extracted_username = extract_username(user_info)
        Log.info { "OAuth2 userinfo authentication successful for user: #{extracted_username}" }

        # Create UserinfoUser with default permissions
        # Token expiration unknown for opaque tokens, set to 1 hour from now
        expires_at = Time.utc + 1.hour
        tags = Array(Tag).new
        permissions = create_default_permissions

        UserinfoUser.new(extracted_username, tags, permissions, expires_at)
      rescue ex : Exception
        Log.warn { "OAuth2 userinfo authentication failed for user \"#{username}\": #{ex.message}" }
        nil
      end

      # Validates token by calling the userinfo endpoint
      private def validate_token(token : String) : JSON::Any?
        headers = ::HTTP::Headers{
          "Authorization" => "Bearer #{token}",
          "Accept"        => "application/json",
          "User-Agent"    => "LavinMQ-OAuth2-Userinfo",
        }

        # Security: Don't log the token itself, only the endpoint
        Log.debug { "Validating OAuth2 token at userinfo endpoint" }

        uri = URI.parse(@config.oauth_userinfo_url)
        ::HTTP::Client.new(uri) do |client|
          client.connect_timeout = 5.seconds
          client.read_timeout = 10.seconds
          response = client.get(uri.request_target, headers: headers)

          if response.success?
            # Validate JSON before parsing
            JSON.parse(response.body)
          else
            Log.warn { "Userinfo endpoint returned status #{response.status_code}" }
            nil
          end
        end
      rescue JSON::ParseException
        Log.error { "Invalid JSON response from userinfo endpoint" }
        nil
      rescue ex
        Log.error(exception: ex) { "Error calling userinfo endpoint: #{ex.message}" }
        nil
      end

      # Extracts username from userinfo response using configured claim
      private def extract_username(user_info : JSON::Any) : String
        claim = @config.oauth_userinfo_username_claim

        # Fix: Access claim only once and handle both string and integer values
        username_value = user_info[claim]?.try do |value|
          value.as_s? || value.as_i64?.try(&.to_s)
        end

        if username_value.nil?
          raise "Could not find claim '#{claim}' in userinfo response"
        end

        # Apply prefix if configured
        prefix = @config.oauth_userinfo_username_prefix
        prefix.empty? ? username_value : "#{prefix}#{username_value}"
      end

      # Creates default permissions for userinfo-authenticated users
      private def create_default_permissions : Hash(String, User::Permissions)
        default_vhost = @config.oauth_userinfo_default_vhost

        {
          default_vhost => {
            config: /.*/,
            read:   /.*/,
            write:  /.*/,
          },
        }
      end

    end
  end
end
