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

      def initialize(@authenticator : Auth::Authenticator)
        register_routes
      end

      private def register_routes
        get "/oauth/enabled" { |context, _params| handle_enabled(context) }
        get "/oauth/authorize" { |context, _params| handle_authorize(context) }
        get "/oauth/callback" { |context, _params| handle_callback(context) }
      end

      private def handle_enabled(context) : ::HTTP::Server::Context
        config = Config.instance
        enabled = !!(config.oauth_client_id && config.oauth_issuer_url)
        context.response.content_type = "application/json"
        {enabled: enabled}.to_json(context.response)
        context
      end

      private def handle_authorize(context) : ::HTTP::Server::Context
        config = Config.instance
        issuer_url = config.oauth_issuer_url || oauth_error(context, 503, "OAuth not configured")
        client_id = config.oauth_client_id || oauth_error(context, 503, "OAuth not configured")

        oidc = Auth::JWT::JWKSFetcher.new(issuer_url, config.oauth_jwks_cache_ttl).fetch_oidc_config
        auth_ep = oidc.authorization_endpoint || raise "OIDC missing authorization_endpoint"

        verifier, challenge = OAuth2::PKCE.generate
        state = Random::Secure.urlsafe_base64(32)

        context.response.cookies << ::HTTP::Cookie.new(
          name: "oauth_state",
          value: "#{state}:#{verifier}",
          path: "/oauth",
          http_only: true,
          secure: true,
          samesite: ::HTTP::Cookie::SameSite::Lax,
          max_age: 5.minutes
        )

        redirect_uri = build_redirect_uri(context.request)
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
        issuer_url = config.oauth_issuer_url || oauth_redirect_error(context, "OAuth not configured")
        client_id = config.oauth_client_id || oauth_redirect_error(context, "OAuth not configured")

        cookie_value = context.request.cookies["oauth_state"]?.try(&.value) || oauth_redirect_error(context, "Missing OAuth state")
        sep = cookie_value.index(':') || oauth_redirect_error(context, "Invalid OAuth state cookie")
        cookie_state = cookie_value[0...sep]
        code_verifier = cookie_value[sep + 1..]

        oauth_redirect_error(context, "State mismatch") unless context.request.query_params["state"]? == cookie_state
        code = context.request.query_params["code"]? || oauth_redirect_error(context, "Missing authorization code")

        oidc = Auth::JWT::JWKSFetcher.new(issuer_url, config.oauth_jwks_cache_ttl).fetch_oidc_config
        token_ep = oidc.token_endpoint || raise "OIDC missing token_endpoint"

        redirect_uri = build_redirect_uri(context.request)
        token_response = OAuth2::TokenExchange.new(token_ep, client_id).exchange(code, redirect_uri, code_verifier)

        auth_context = Auth::Context.new("", token_response.access_token.to_slice, context.request.remote_address)
        oauth_redirect_error(context, "Token validation failed") unless @authenticator.authenticate(auth_context)

        context.response.cookies << ::HTTP::Cookie.new(
          name: "m",
          value: token_response.access_token,
          path: "/",
          secure: true,
          samesite: ::HTTP::Cookie::SameSite::Strict,
          max_age: 8.hours
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

      private def build_redirect_uri(request : ::HTTP::Request) : String
        if base_url = Config.instance.oauth_mgmt_base_url
          "#{base_url.chomp("/")}/oauth/callback"
        else
          host = request.headers["Host"]? || "localhost"
          scheme = request.headers["X-Forwarded-Proto"]? || "http"
          "#{scheme}://#{host}/oauth/callback"
        end
      end

      class OAuthError < Exception; end
    end
  end
end
