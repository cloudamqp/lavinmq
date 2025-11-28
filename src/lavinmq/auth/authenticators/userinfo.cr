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
    # 3. If successful, extracts username and creates OAuthUser with default permissions
    # 4. User gets full access to configured default vhost
    #
    # This is simpler than JWT but requires an HTTP call per authentication.
    # Consider using with token caching for better performance.
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

        Log.debug { "Validating OAuth2 token at userinfo endpoint: #{@config.oauth_userinfo_url}" }

        uri = URI.parse(@config.oauth_userinfo_url)
        ::HTTP::Client.new(uri) do |client|
          client.connect_timeout = 5.seconds
          client.read_timeout = 10.seconds
          response = client.get(uri.request_target, headers: headers)

          if response.success?
            JSON.parse(response.body)
          else
            Log.warn { "Userinfo endpoint returned status #{response.status_code}: #{response.body}" }
            nil
          end
        end
      rescue ex
        Log.error(exception: ex) { "Error calling userinfo endpoint: #{ex.message}" }
        nil
      end

      # Extracts username from userinfo response using configured claim
      private def extract_username(user_info : JSON::Any) : String
        claim = @config.oauth_userinfo_username_claim
        username_value = user_info[claim]?.try(&.as_s) || user_info[claim]?.try(&.as_i64.to_s)

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
