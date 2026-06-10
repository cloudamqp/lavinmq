require "../spec_helper"
require "../../src/lavinmq/http/oauth2/pkce"
require "../../src/lavinmq/auth/jwt/jwks_fetcher"

# Test-only fetcher that lets specs pre-seed the OIDC config and public keys
# without going over the network.
class SeedableJWKSFetcher < LavinMQ::Auth::JWT::JWKSFetcher
  def initialize(oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration,
                 keys : Hash(String, String) = {} of String => String)
    super(URI.parse("http://seedable.invalid"), 1.hour)
    @oidc_config = oidc
    @public_keys.update(keys, 1.hour) unless keys.empty?
  end
end

class OnDemandOIDCFetcher < LavinMQ::Auth::JWT::JWKSFetcher
  def initialize(@discovered_oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration)
    super(URI.parse(@discovered_oidc.issuer), 1.hour)
  end

  def fetch_oidc_config : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration
    @oidc_config = @discovered_oidc
  end
end

class FakeBaseUser < LavinMQ::Auth::BaseUser
  getter name : String
  getter tags : Array(LavinMQ::Tag)
  getter permissions : Hash(String, LavinMQ::Auth::BaseUser::Permissions) = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

  def initialize(@name : String, @tags : Array(LavinMQ::Tag) = [LavinMQ::Tag::Administrator])
  end
end

# OAuthAuthenticator subclass that returns a hard-coded user so specs can drive
# the /oauth/callback success path without real JWT signing.
class StubOAuthAuthenticator < LavinMQ::Auth::OAuthAuthenticator
  def initialize(@user : LavinMQ::Auth::BaseUser, fetcher : LavinMQ::Auth::JWT::JWKSFetcher)
    super(LavinMQ::Auth::JWT::TokenVerifier.new(LavinMQ::Config.instance, fetcher))
  end

  def authenticate(context : LavinMQ::Auth::Context) : LavinMQ::Auth::BaseUser?
    @user
  end
end

def build_oauth_chain(oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration,
                      keys = {} of String => String) : LavinMQ::Auth::Chain
  fetcher = SeedableJWKSFetcher.new(oidc, keys)
  verifier = LavinMQ::Auth::JWT::TokenVerifier.new(LavinMQ::Config.instance, fetcher)
  oauth_auth = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
  LavinMQ::Auth::Chain.new([oauth_auth] of LavinMQ::Auth::Authenticator)
end

def build_stub_oauth_chain(user : LavinMQ::Auth::BaseUser,
                           oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration) : LavinMQ::Auth::Chain
  fetcher = SeedableJWKSFetcher.new(oidc)
  oauth_auth = StubOAuthAuthenticator.new(user, fetcher)
  LavinMQ::Auth::Chain.new([oauth_auth] of LavinMQ::Auth::Authenticator)
end

def default_oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration
  LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
    issuer: "https://idp.example.com",
    jwks_uri: "https://idp.example.com/jwks",
    token_endpoint: "https://idp.example.com/token")
end

def start_stub_idp(&block : ::HTTP::Server::Context ->) : {::HTTP::Server, Socket::IPAddress}
  server = ::HTTP::Server.new(&block)
  addr = server.bind_tcp("127.0.0.1", 0)
  spawn { server.listen }
  Fiber.yield
  {server, addr}
end

describe "OAuth2" do
  describe "OAuthController when management UI SSO is not configured" do
    it "GET /oauth/authorize returns 503" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 503
        response.body.should contain("OAuth not configured")
      end
    end

    it "GET /oauth/callback redirects to login" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/callback"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 302
        response.headers["Location"].should contain("OAuth%20not%20configured")
      end
    end
  end

  describe "OAuthController /oauth/callback error handling" do
    it "redirects to login when oauth_state cookie is missing" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=abc123&code=authcode"),
          headers: ::HTTP::Headers.new
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("Missing%20OAuth%20state")
      end
    end

    it "redirects to login on state mismatch" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        headers = ::HTTP::Headers{
          "Cookie" => "oauth_state=correct_state:verifier123",
        }
        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=wrong_state&code=authcode"),
          headers: headers
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("State%20mismatch")
      end
    end

    it "redirects to login when code query parameter is missing" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        headers = ::HTTP::Headers{
          "Cookie" => "oauth_state=mystate:verifier123",
        }
        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=mystate"),
          headers: headers
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("Missing%20authorization%20code")
      end
    end
  end

  describe "OAuthController JWT cookie" do
    it "rejects an invalid JWT in the oauth_token cookie with 401" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        headers = ::HTTP::Headers{
          "Cookie" => "oauth_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fakepayload.fakesignature",
        }
        response = ::HTTP::Client.get(http.test_uri("/api/whoami"), headers: headers)
        response.status_code.should eq 401
      end
    end

    it "clears a stale oauth_token cookie on rejection" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        headers = ::HTTP::Headers{
          "Cookie" => "oauth_token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fakepayload.fakesignature",
        }
        response = ::HTTP::Client.get(http.test_uri("/api/whoami"), headers: headers)
        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        cleared = set_cookies.any? { |c| c.starts_with?("oauth_token=;") && c.downcase.includes?("max-age=0") }
        cleared.should be_true
      end
    end

    it "still allows basic auth" do
      with_http_server do |http, _|
        response = http.get("/api/whoami")
        response.status_code.should eq 200
      end
    end
  end

  describe "PKCE" do
    it "generates a verifier of at least 43 characters" do
      verifier, _ = LavinMQ::HTTP::OAuth2::PKCE.generate
      verifier.size.should be >= 43
    end

    it "generates a correct challenge (SHA256 of verifier, base64url-encoded)" do
      verifier, challenge = LavinMQ::HTTP::OAuth2::PKCE.generate
      expected = Base64.urlsafe_encode(
        OpenSSL::Digest.new("SHA256").update(verifier).final,
        padding: false
      )
      challenge.should eq expected
    end

    it "produces different verifiers on each call" do
      verifier1, _ = LavinMQ::HTTP::OAuth2::PKCE.generate
      verifier2, _ = LavinMQ::HTTP::OAuth2::PKCE.generate
      verifier1.should_not eq verifier2
    end
  end

  describe "OAuthController /oauth/enabled" do
    it "returns enabled=false when OAuth is not configured" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/enabled"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        JSON.parse(response.body)["enabled"].as_bool.should be_false
      end
    end

    it "returns enabled=true when OAuth is configured" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        response = ::HTTP::Client.get(http.test_uri("/oauth/enabled"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        JSON.parse(response.body)["enabled"].as_bool.should be_true
      end
    end
  end

  # Note: aud-claim-mismatch rejection is unit-tested in spec/token_verifier_spec.cr
  # (describe "#validate_audience"). At the /oauth/callback integration level we
  # cover the same user-facing outcome (redirect to /login with an error) via
  # the "token validation failure" test below, since the authenticator returns
  # nil for any validation failure.
  describe "OAuthController /oauth/callback integration" do
    it "redirects to login when token exchange returns non-2xx" do
      idp_server, idp_addr = start_stub_idp do |ctx|
        ctx.response.status_code = 500
        ctx.response.print %({"error":"internal"})
      end
      begin
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
          issuer: "https://idp.example.com",
          jwks_uri: "http://#{idp_addr}/jwks",
          token_endpoint: "http://#{idp_addr}/token")
        chain = build_oauth_chain(oidc)

        with_http_server(authenticator: chain) do |http, _|
          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)
          response.status_code.should eq 302
          response.headers["Location"].should start_with("../login?error=")
        end
      ensure
        idp_server.close
      end
    end

    it "redirects to login with error when token validation fails" do
      idp_server, idp_addr = start_stub_idp do |ctx|
        ctx.response.content_type = "application/json"
        ctx.response.print %({"access_token":"not-a-valid-jwt","expires_in":3600})
      end
      begin
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
          issuer: "https://idp.example.com",
          jwks_uri: "http://#{idp_addr}/jwks",
          token_endpoint: "http://#{idp_addr}/token")
        chain = build_oauth_chain(oidc)

        with_http_server(authenticator: chain) do |http, _|
          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)
          response.status_code.should eq 302
          response.headers["Location"].should contain("Token%20validation%20failed")
        end
      ensure
        idp_server.close
      end
    end
  end

  describe "OAuthController /oauth/callback success cookies" do
    it "sets oauth_token (HttpOnly, SameSite=Lax) and m identity cookie with Secure" do
      idp_server, idp_addr = start_stub_idp do |ctx|
        ctx.response.content_type = "application/json"
        ctx.response.print %({"access_token":"dummy","expires_in":3600})
      end
      begin
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

        oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
          issuer: "https://idp.example.com",
          jwks_uri: "http://#{idp_addr}/jwks",
          token_endpoint: "http://#{idp_addr}/token")
        chain = build_stub_oauth_chain(FakeBaseUser.new("alice@example.com"), oidc)

        with_http_server(authenticator: chain) do |http, _|
          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)
          response.status_code.should eq 302
          response.headers["Location"].should eq("..")

          set_cookies = response.headers.get?("Set-Cookie") || [] of String
          token_cookie = set_cookies.find { |c| c.starts_with?("oauth_token=") && !c.starts_with?("oauth_token=;") }
          identity_cookie = set_cookies.find(&.starts_with?("m=|oauth:"))

          token_cookie.should_not be_nil
          token_lower = token_cookie.not_nil!.downcase
          token_lower.should contain("secure")
          token_lower.should contain("httponly")
          token_lower.should contain("samesite=lax")

          identity_cookie.should_not be_nil
          identity_lower = identity_cookie.not_nil!.downcase
          identity_lower.should contain("secure")
          identity_lower.should contain("samesite=lax")
          identity_lower.should_not contain("httponly")
          identity_cookie.not_nil!.should contain("YWxpY2VAZXhhbXBsZS5jb206")
        end
      ensure
        idp_server.close
      end
    end

    it "scopes cookies to the path of oauth_mgmt_base_url" do
      idp_server, idp_addr = start_stub_idp do |ctx|
        ctx.response.content_type = "application/json"
        ctx.response.print %({"access_token":"dummy","expires_in":3600})
      end
      begin
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672/lavinmq")

        oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
          issuer: "https://idp.example.com",
          jwks_uri: "http://#{idp_addr}/jwks",
          token_endpoint: "http://#{idp_addr}/token")
        chain = build_stub_oauth_chain(FakeBaseUser.new("alice@example.com"), oidc)

        with_http_server(authenticator: chain) do |http, _|
          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)

          set_cookies = response.headers.get?("Set-Cookie") || [] of String
          token_cookie = set_cookies.find { |c| c.starts_with?("oauth_token=") && !c.starts_with?("oauth_token=;") }
          identity_cookie = set_cookies.find(&.starts_with?("m=|oauth:"))

          token_cookie.not_nil!.should contain("path=/lavinmq")
          identity_cookie.not_nil!.should contain("path=/lavinmq")
        end
      ensure
        idp_server.close
      end
    end
  end

  describe "OAuthController /oauth/authorize cookie flags" do
    it "fetches OIDC discovery on demand when the background refresh has not populated it yet" do
      LavinMQ::Config.instance.oauth_client_id = "test-client"
      LavinMQ::Config.instance.oauth_issuer_url = URI.parse("http://seedable.invalid")
      LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

      oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
        issuer: "http://seedable.invalid",
        jwks_uri: "https://idp.example.com/jwks",
        authorization_endpoint: "https://idp.example.com/authorize",
        token_endpoint: "https://idp.example.com/token")
      fetcher = OnDemandOIDCFetcher.new(oidc)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(LavinMQ::Config.instance, fetcher)
      chain = LavinMQ::Auth::Chain.new([LavinMQ::Auth::OAuthAuthenticator.new(verifier)] of LavinMQ::Auth::Authenticator)

      with_http_server(authenticator: chain) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        JSON.parse(response.body)["authorize_url"].as_s.should start_with("https://idp.example.com/authorize?")
      end
    end

    it "sets oauth_state with Secure, HttpOnly, SameSite=Lax" do
      LavinMQ::Config.instance.oauth_client_id = "test-client"
      LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
      LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")

      oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
        issuer: "https://idp.example.com",
        jwks_uri: "https://idp.example.com/jwks",
        authorization_endpoint: "https://idp.example.com/authorize",
        token_endpoint: "https://idp.example.com/token")
      chain = build_oauth_chain(oidc)

      with_http_server(authenticator: chain) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        state_cookie = set_cookies.find(&.starts_with?("oauth_state="))
        state_cookie.should_not be_nil
        state_cookie.not_nil!.downcase.should contain("secure")
        state_cookie.not_nil!.downcase.should contain("httponly")
        state_cookie.not_nil!.downcase.should contain("samesite=lax")
        # Path must be "/" so the state cookie is sent back on the callback even
        # when the UI is mounted under a reverse-proxy base path (a stripping
        # proxy means the server can't know the prefix; "/oauth" would not match
        # the public /<prefix>/oauth/callback path).
        state_cookie.not_nil!.downcase.should contain("path=/")
        state_cookie.not_nil!.downcase.should_not contain("path=/oauth")
      end
    end

    it "includes the audience parameter when oauth_audience is configured" do
      LavinMQ::Config.instance.oauth_client_id = "test-client"
      LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
      LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")
      LavinMQ::Config.instance.oauth_audience = "lavinmq-api"

      oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
        issuer: "https://idp.example.com",
        jwks_uri: "https://idp.example.com/jwks",
        authorization_endpoint: "https://idp.example.com/authorize",
        token_endpoint: "https://idp.example.com/token")
      chain = build_oauth_chain(oidc)

      with_http_server(authenticator: chain) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        authorize_url = JSON.parse(response.body)["authorize_url"].as_s
        URI.parse(authorize_url).query_params["audience"].should eq "lavinmq-api"
      end
    ensure
      LavinMQ::Config.instance.oauth_audience = nil
    end

    it "omits the audience parameter when oauth_audience is not configured" do
      LavinMQ::Config.instance.oauth_client_id = "test-client"
      LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
      LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("https://localhost:15672")
      LavinMQ::Config.instance.oauth_audience = nil

      oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
        issuer: "https://idp.example.com",
        jwks_uri: "https://idp.example.com/jwks",
        authorization_endpoint: "https://idp.example.com/authorize",
        token_endpoint: "https://idp.example.com/token")
      chain = build_oauth_chain(oidc)

      with_http_server(authenticator: chain) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        authorize_url = JSON.parse(response.body)["authorize_url"].as_s
        URI.parse(authorize_url).query_params.has_key?("audience").should be_false
      end
    end

    it "omits Secure when the management UI is served over http://localhost" do
      LavinMQ::Config.instance.oauth_client_id = "test-client"
      LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
      LavinMQ::Config.instance.oauth_mgmt_base_url = URI.parse("http://localhost:15672")

      oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
        issuer: "https://idp.example.com",
        jwks_uri: "https://idp.example.com/jwks",
        authorization_endpoint: "https://idp.example.com/authorize",
        token_endpoint: "https://idp.example.com/token")
      chain = build_oauth_chain(oidc)

      with_http_server(authenticator: chain) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        state_cookie = set_cookies.find(&.starts_with?("oauth_state="))
        state_cookie.should_not be_nil
        state_cookie.not_nil!.downcase.should_not contain("secure")
      end
    end
  end

  describe "OAuthController /oauth/logout" do
    it "clears the oauth cookies and redirects to login" do
      with_http_server(authenticator: build_oauth_chain(default_oidc)) do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/logout"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 302
        response.headers["Location"].should eq("../login")

        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        token_cleared = set_cookies.any? { |c| c.starts_with?("oauth_token=;") && c.downcase.includes?("max-age=0") }
        identity_cleared = set_cookies.any? { |c| c.starts_with?("m=;") && c.downcase.includes?("max-age=0") }
        token_cleared.should be_true
        identity_cleared.should be_true
      end
    end
  end
end
