require "../auth_handler"
require "../jwt_validator"
require "jwt"
require "../../config"
require "http/client"
require "json"

module LavinMQ
  class OAuth2Handler < LavinMQ::AuthHandler
    @jwks_client : JWKSClient?
    @jwt_validator : JWTValidator?

    def initialize(@users : UserStore)
      @config = LavinMQ::Config.instance
      initialize_jwt_validation
    end

    private def initialize_jwt_validation
      # Initialize JWT validation if JWKS URL is configured
      if !@config.jwks_uri.empty?
        @jwks_client = JWKSClient.new(
          @config.jwks_uri,
          cache_ttl: @config.oauth2_jwks_cache_ttl.seconds
        )

        @jwt_validator = JWTValidator.new(
          @jwks_client.not_nil!,
          required_issuer: @config.iss.presence,
          required_audience: @config.aud.presence,
          clock_skew: @config.token_expiration_tolerance.seconds
        )

        @log.info { "JWT validation initialized with JWKS URL: #{@config.jwks_uri}" }
      end
    rescue ex
      @log.warn { "Failed to initialize JWT validation: #{ex.message}" }
    end

    def authenticate(username : String, password : String)
      token = password

      # Check if OAuth2 is configured at all
      unless oauth2_enabled?
        @log.debug { "OAuth2 not configured, skipping" }
        return try_next(username, password)
      end

      # Strategy 1: Try JWT validation first (fastest, no HTTP call)
      if jwt_enabled? && looks_like_jwt?(token)
        @log.debug { "Attempting JWT validation" }
        if user = validate_jwt(token)
          @log.info { "OAuth2 JWT authentication successful for user: #{user.name}" }
          return user
        end
        @log.debug { "JWT validation failed, trying userinfo fallback" }
      end

      # Strategy 2: Fall back to userinfo endpoint validation
      if userinfo_enabled?
        @log.debug { "Attempting userinfo validation" }
        if user = validate_userinfo(token)
          @log.info { "OAuth2 userinfo authentication successful for user: #{user.name}" }
          return user
        end
      end

      # Strategy 3: Pass to next handler
      @log.warn { "OAuth2 authentication failed for all methods" }
      try_next(username, password)
    rescue ex
      @log.warn { "OAuth2 authentication error: #{ex.message}" }
      try_next(username, password)
    end

    private def oauth2_enabled? : Bool
      jwt_enabled? || userinfo_enabled?
    end

    private def jwt_enabled? : Bool
      !@config.jwks_uri.empty? && !@jwt_validator.nil?
    end

    private def userinfo_enabled? : Bool
      !@config.oauth2_userinfo_url.empty?
    end

    private def looks_like_jwt?(token : String) : Bool
      # JWT tokens have 3 parts separated by dots
      parts = token.split('.')
      parts.size == 3
    end

    private def validate_jwt(token : String) : User?
      validator = @jwt_validator
      return nil unless validator

      # Validate token and extract claims
      claims = validator.validate(token)
      return nil unless claims

      @log.debug { "JWT validated: sub=#{claims.sub}, iss=#{claims.iss}" }

      # Extract username from claims
      username = extract_username_from_claims(claims)

      # Create or get user with permissions from token
      create_or_get_user_from_claims(username, claims)
    rescue ex
      @log.error { "JWT validation error: #{ex.message}" }
      nil
    end

    private def extract_username_from_claims(claims : TokenClaims) : String
      # Try preferred_username first, then email, then sub
      username_value = claims.preferred_username || claims.email || claims.sub

      # Apply configured prefix
      prefix = @config.oauth2_username_prefix
      prefix.empty? ? username_value : "#{prefix}#{username_value}"
    end

    private def create_or_get_user_from_claims(username : String, claims : TokenClaims) : User?
      # Check if user already exists
      if existing_user = @users[username]?
        @log.debug { "Returning existing OAuth2 user: #{username}" }
        return existing_user
      end

      # Create new temporary user (not saved to disk)
      user = @users.create(username, Random::Secure.hex(16), save: false)

      # Apply permissions from token claims or defaults
      apply_permissions_from_claims(user, claims)

      @log.info { "Created temporary OAuth2 user from JWT: #{username}" }
      user
    rescue ex
      @log.error { "Error creating OAuth2 user from JWT: #{ex.message}" }
      nil
    end

    private def apply_permissions_from_claims(user : User, claims : TokenClaims)
      # If permissions are in the token, use them
      if token_perms = claims.permissions
        token_perms.each do |vhost, perms|
          user.permissions[vhost] = {
            config: Regex.new(perms[:configure]),
            write:  Regex.new(perms[:write]),
            read:   Regex.new(perms[:read]),
          }
        end
        return
      end

      # If scope is present, parse it for permissions
      if scope = claims.scope
        if perms = parse_scope_permissions(scope)
          perms.each do |vhost, perm|
            user.permissions[vhost] = perm
          end
          return
        end
      end

      # Fall back to default permissions
      apply_default_permissions(user)
    end

    private def parse_scope_permissions(scope : String) : Hash(String, NamedTuple(config: String, write: String, read: String))?
      # Parse RabbitMQ-style scope permissions
      # Format: "rabbitmq.read:*/* rabbitmq.write:*/* rabbitmq.configure:*/queue-*"
      # TODO: Implement full RabbitMQ scope parsing
      # For now, just check for basic scopes
      nil
    end

    private def apply_default_permissions(user : User)
      # Apply default permissions from config
      default_vhost = @config.oauth2_default_vhost || "/"

      user.permissions[default_vhost] = {
        config: /.*/,
        write:  /.*/,
        read:   /.*/,
      }
    end

    # ===== Userinfo Endpoint Validation (Fallback) =====

    private def validate_userinfo(token : String) : User?
      user_info = validate_token(@config.oauth2_userinfo_url, token)
      return nil unless user_info

      username = extract_username(user_info)
      create_or_get_user(username, user_info)
    rescue ex
      @log.error { "Userinfo validation error: #{ex.message}" }
      nil
    end

    private def validate_token(userinfo_url : String, token : String) : JSON::Any?
      # Call the configured userinfo endpoint to validate the token
      headers = ::HTTP::Headers{
        "Authorization" => "Bearer #{token}",
        "Accept"        => "application/json",
        "User-Agent"    => "LavinMQ-OAuth2",
      }

      @log.debug { "Validating OAuth2 token at: #{userinfo_url}" }
      response = ::HTTP::Client.get(userinfo_url, headers: headers)

      if response.status.success?
        JSON.parse(response.body)
      else
        @log.warn { "OAuth2 userinfo endpoint returned status #{response.status}: #{response.body}" }
        nil
      end
    rescue ex
      @log.error { "Error calling OAuth2 userinfo endpoint: #{ex.message}" }
      nil
    end

    private def extract_username(user_info : JSON::Any) : String
      # Extract username from the configured claim
      claim = @config.oauth2_username_claim
      username_value = user_info[claim]?.try(&.as_s) || user_info[claim]?.try(&.as_i64.to_s)

      if username_value.nil?
        raise "Could not find claim '#{claim}' in userinfo response"
      end

      # Apply prefix if configured
      prefix = @config.oauth2_username_prefix
      prefix.empty? ? username_value : "#{prefix}#{username_value}"
    end

    private def create_or_get_user(username : String, user_info : JSON::Any) : User?
      # Check if user already exists
      if existing_user = @users[username]?
        @log.debug { "Returning existing OAuth2 user: #{username}" }
        return existing_user
      end

      # Create new temporary user (not saved to disk)
      # Use a random string as password since it won't be used for authentication
      user = @users.create(username, Random::Secure.hex(16), save: false)

      # Set default permissions for the user
      # For now, give full permissions to default vhost
      # TODO: Make this configurable or extract from token claims
      user.permissions["/"] = {
        config: /.*/,
        write:  /.*/,
        read:   /.*/,
      }

      @log.info { "Created temporary OAuth2 user: #{username}" }
      user
    rescue ex
      @log.error { "Error creating OAuth2 user: #{ex.message}" }
      nil
    end
  end
end
