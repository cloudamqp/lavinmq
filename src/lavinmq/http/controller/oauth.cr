require "../controller"
require "../router"
require "../oauth2/pkce"
require "../oauth2/token_exchange"
require "../../auth/jwt/jwks_fetcher"

module LavinMQ
  module HTTP
    class OAuthController
      include Router

      Log = LavinMQ::Log.for "http.oauth"

      @oidc_config : Auth::JWT::JWKSFetcher::OIDCConfiguration?
      @oidc_mutex = Mutex.new

      def initialize(@authenticator : Auth::Authenticator)
        register_routes
      end

      private def register_routes
        get "/oauth/enabled" { |context, _params| handle_enabled(context) }
        get "/oauth/authorize" { |context, _params| handle_authorize(context) }
        get "/oauth/callback" { |context, _params| handle_callback(context) }
        get "/oauth/logout" { |context, _params| handle_logout(context) }
      end

      private def handle_enabled(context) : ::HTTP::Server::Context
        config = Config.instance
        enabled = !!(config.oauth_client_id && config.oauth_issuer_url && config.oauth_mgmt_base_url)
        context.response.content_type = "application/json"
        {enabled: enabled}.to_json(context.response)
        context
      end

      private def handle_authorize(context) : ::HTTP::Server::Context
        config = Config.instance
        oauth_error(context, 503, "OAuth not configured") unless config.oauth_issuer_url
        client_id = config.oauth_client_id || oauth_error(context, 503, "OAuth not configured")

        auth_ep = oidc_config.authorization_endpoint || raise "OIDC missing authorization_endpoint"
        verifier, challenge = OAuth2::PKCE.generate
        state = Random::Secure.urlsafe_base64(32)

        set_cookie(context, "oauth_state", "#{state}:#{verifier}",
          path: "/oauth", http_only: true,
          samesite: ::HTTP::Cookie::SameSite::Lax, max_age: 5.minutes)

        params = ::URI::Params.build do |p|
          p.add "client_id", client_id
          p.add "redirect_uri", build_redirect_uri
          p.add "response_type", "code"
          p.add "scope", "openid profile"
          p.add "code_challenge", challenge
          p.add "code_challenge_method", "S256"
          p.add "state", state
        end

        context.response.content_type = "application/json"
        {authorize_url: "#{auth_ep}?#{params}"}.to_json(context.response)
        context
      rescue OAuthError
        context
      rescue ex
        Log.error(exception: ex) { "OAuth authorize failed: #{ex.message}" }
        context.response.status_code = 502
        context.response.content_type = "application/json"
        {reason: "OAuth authorization failed"}.to_json(context.response)
        context
      end

      private def handle_callback(context) : ::HTTP::Server::Context
        code, code_verifier, client_id = validate_callback(context)

        token_ep = oidc_config.token_endpoint || raise "OIDC missing token_endpoint"
        token_response = OAuth2::TokenExchange.new(token_ep, client_id)
          .exchange(code, build_redirect_uri, code_verifier)

        auth_context = Auth::Context.new("", token_response.access_token.to_slice, context.request.remote_address)
        user = @authenticator.authenticate(auth_context)
        oauth_redirect_error(context, "Token validation failed") if user.nil? || user.tags.empty?

        max_age = token_response.expires_in.try { |e| Math.min(e, 8.hours.total_seconds.to_i64).seconds } || 8.hours

        set_cookie(context, "m", token_response.access_token,
          path: "/", http_only: true, samesite: ::HTTP::Cookie::SameSite::Strict, max_age: max_age)
        set_cookie(context, "oauth_user", extract_username(token_response.access_token),
          path: "/", samesite: ::HTTP::Cookie::SameSite::Strict, max_age: max_age)
        expire_cookie(context, "oauth_state", "/oauth")

        context.response.status = ::HTTP::Status::FOUND
        context.response.headers["Location"] = "/"
        context
      rescue OAuthError
        context
      rescue ex
        Log.error(exception: ex) { "OAuth callback failed: #{ex.message}" }
        context.response.status = ::HTTP::Status::FOUND
        context.response.headers["Location"] = "/login?error=#{URI.encode_path_segment("OAuth authentication failed")}"
        context
      end

      private def handle_logout(context) : ::HTTP::Server::Context
        expire_cookie(context, "m", "/")
        expire_cookie(context, "oauth_user", "/")
        context.response.status = ::HTTP::Status::FOUND
        context.response.headers["Location"] = "/login"
        context
      end

      private def validate_callback(context) : {String, String, String}
        config = Config.instance
        oauth_redirect_error(context, "OAuth not configured") unless config.oauth_issuer_url
        client_id = config.oauth_client_id || oauth_redirect_error(context, "OAuth not configured")

        if error = context.request.query_params["error"]?
          description = context.request.query_params["error_description"]? || error
          oauth_redirect_error(context, description)
        end

        cookie_value = context.request.cookies["oauth_state"]?.try(&.value) || oauth_redirect_error(context, "Missing OAuth state")
        sep = cookie_value.index(':') || oauth_redirect_error(context, "Invalid OAuth state cookie")
        oauth_redirect_error(context, "State mismatch") unless context.request.query_params["state"]? == cookie_value[0...sep]
        code = context.request.query_params["code"]? || oauth_redirect_error(context, "Missing authorization code")

        {code, cookie_value[sep + 1..], client_id}
      end

      private def set_cookie(context, name, value, path = "/", http_only = false,
                             samesite = ::HTTP::Cookie::SameSite::Strict,
                             max_age = 8.hours)
        context.response.cookies << ::HTTP::Cookie.new(
          name: name, value: value, path: path, http_only: http_only,
          secure: secure_cookie?, samesite: samesite, max_age: max_age)
      end

      private def expire_cookie(context, name, path)
        context.response.cookies << ::HTTP::Cookie.new(name: name, value: "", path: path, max_age: 0.seconds)
      end

      private def oauth_error(context, status_code, message) : NoReturn
        context.response.status_code = status_code
        context.response.content_type = "application/json"
        {reason: message}.to_json(context.response)
        raise OAuthError.new
      end

      private def oauth_redirect_error(context, message) : NoReturn
        context.response.status = ::HTTP::Status::FOUND
        context.response.headers["Location"] = "/login?error=#{URI.encode_path_segment(message)}"
        raise OAuthError.new
      end

      private def extract_username(access_token : String) : String
        parts = access_token.split('.')
        return "SSO User" unless parts.size == 3
        payload = JSON.parse(Auth::JWT::RS256Parser.base64url_decode(parts[1]))
        Config.instance.oauth_preferred_username_claims.each do |claim|
          if value = payload[claim]?.try(&.as_s?)
            return URI.encode_www_form(value)
          end
        end
        "SSO User"
      rescue ex
        Log.debug(exception: ex) { "Could not extract username from JWT" }
        "SSO User"
      end

      # Cached for the lifetime of the process. OIDC endpoints are essentially
      # static; a server restart is needed if the IdP changes them.
      private def oidc_config : Auth::JWT::JWKSFetcher::OIDCConfiguration
        @oidc_mutex.synchronize do
          @oidc_config ||= begin
            config = Config.instance
            issuer_url = config.oauth_issuer_url || raise "OAuth issuer not configured"
            Auth::JWT::JWKSFetcher.new(issuer_url, config.oauth_jwks_cache_ttl).fetch_oidc_config
          end
        end
      end

      private def build_redirect_uri : String
        base_url = Config.instance.oauth_mgmt_base_url || raise "oauth_mgmt_base_url must be configured when OAuth is enabled"
        "#{base_url.chomp("/")}/oauth/callback"
      end

      private def secure_cookie? : Bool
        Config.instance.oauth_mgmt_base_url.try(&.starts_with?("https")) || false
      end

      class OAuthError < Exception; end
    end
  end
end
