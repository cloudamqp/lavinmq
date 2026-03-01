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

        context.response.cookies << ::HTTP::Cookie.new(
          name: "oauth_state",
          value: "#{state}:#{verifier}",
          path: "/oauth",
          http_only: true,
          secure: secure_cookie?,
          samesite: ::HTTP::Cookie::SameSite::Lax,
          max_age: 5.minutes
        )

        redirect_uri = build_redirect_uri
        params = ::URI::Params.build do |p|
          p.add "client_id", client_id
          p.add "redirect_uri", redirect_uri
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
        config = Config.instance
        oauth_redirect_error(context, "OAuth not configured") unless config.oauth_issuer_url
        client_id = config.oauth_client_id || oauth_redirect_error(context, "OAuth not configured")

        cookie_value = context.request.cookies["oauth_state"]?.try(&.value) || oauth_redirect_error(context, "Missing OAuth state")
        sep = cookie_value.index(':') || oauth_redirect_error(context, "Invalid OAuth state cookie")
        cookie_state = cookie_value[0...sep]
        code_verifier = cookie_value[sep + 1..]

        oauth_redirect_error(context, "State mismatch") unless context.request.query_params["state"]? == cookie_state
        code = context.request.query_params["code"]? || oauth_redirect_error(context, "Missing authorization code")

        token_ep = oidc_config.token_endpoint || raise "OIDC missing token_endpoint"

        redirect_uri = build_redirect_uri
        token_response = OAuth2::TokenExchange.new(token_ep, client_id).exchange(code, redirect_uri, code_verifier)

        auth_context = Auth::Context.new("", token_response.access_token.to_slice, context.request.remote_address)
        user = @authenticator.authenticate(auth_context)
        oauth_redirect_error(context, "Token validation failed") unless user && !user.tags.empty?

        cookie_max_age = if expires_in = token_response.expires_in
                           Math.min(expires_in, 8.hours.total_seconds.to_i64).seconds
                         else
                           8.hours
                         end

        context.response.cookies << ::HTTP::Cookie.new(
          name: "m",
          value: token_response.access_token,
          path: "/",
          http_only: true,
          secure: secure_cookie?,
          samesite: ::HTTP::Cookie::SameSite::Strict,
          max_age: cookie_max_age
        )
        context.response.cookies << ::HTTP::Cookie.new(
          name: "oauth_user",
          value: extract_username(token_response.access_token),
          path: "/",
          secure: secure_cookie?,
          samesite: ::HTTP::Cookie::SameSite::Strict,
          max_age: cookie_max_age
        )
        context.response.cookies << ::HTTP::Cookie.new(
          name: "oauth_state",
          value: "",
          path: "/oauth",
          max_age: 0.seconds
        )

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
        context.response.cookies << ::HTTP::Cookie.new(
          name: "m", value: "", path: "/", max_age: 0.seconds
        )
        context.response.cookies << ::HTTP::Cookie.new(
          name: "oauth_user", value: "", path: "/", max_age: 0.seconds
        )
        context.response.status = ::HTTP::Status::FOUND
        context.response.headers["Location"] = "/login"
        context
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
        base64 = parts[1].tr("-_", "+/")
        padded = base64 + "=" * ((4 - base64.size % 4) % 4)
        payload = JSON.parse(Base64.decode_string(padded))
        Config.instance.oauth_preferred_username_claims.each do |claim|
          if value = payload[claim]?.try(&.as_s?)
            return URI.encode_path(value)
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
        @oidc_config ||= begin
          config = Config.instance
          issuer_url = config.oauth_issuer_url || raise "OAuth issuer not configured"
          Auth::JWT::JWKSFetcher.new(issuer_url, config.oauth_jwks_cache_ttl).fetch_oidc_config
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
