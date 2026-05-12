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

class FakeBaseUser < LavinMQ::Auth::BaseUser
  getter name : String
  getter tags : Array(LavinMQ::Tag)
  getter permissions : Hash(String, LavinMQ::Auth::BaseUser::Permissions) = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

  def initialize(@name : String, @tags : Array(LavinMQ::Tag) = [LavinMQ::Tag::Administrator])
  end
end

# Authenticator that returns a hard-coded user so specs can drive the /oauth/callback
# success path without real JWT signing.
class StubOAuthUserAuthenticator < LavinMQ::Auth::Authenticator
  def initialize(@user : LavinMQ::Auth::BaseUser)
  end

  def authenticate(context : LavinMQ::Auth::Context) : LavinMQ::Auth::BaseUser?
    @user
  end

  def cleanup
  end
end

def install_oauth_fetcher(server, oidc : LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration,
                          keys = {} of String => String)
  chain = server.authenticator.as(LavinMQ::Auth::Chain)
  chain.oauth_fetcher = SeedableJWKSFetcher.new(oidc, keys)
end

def start_stub_idp(&block : ::HTTP::Server::Context ->) : {::HTTP::Server, Socket::IPAddress}
  server = ::HTTP::Server.new(&block)
  addr = server.bind_tcp("127.0.0.1", 0)
  spawn { server.listen }
  Fiber.yield
  {server, addr}
end

describe "OAuth2" do
  describe "OAuthController when OAuth is NOT configured" do
    it "GET /oauth/authorize returns 503" do
      with_http_server do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 503
        response.body.should contain("OAuth not configured")
      end
    end

    it "GET /oauth/callback redirects to login" do
      with_http_server do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/callback"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 302
        response.headers["Location"].should contain("OAuth%20not%20configured")
      end
    end
  end

  describe "OAuthController /oauth/callback error handling" do
    it "redirects to login when oauth_state cookie is missing" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = "https://localhost:15672"

        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=abc123&code=authcode"),
          headers: ::HTTP::Headers.new
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("Missing%20OAuth%20state")
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
        LavinMQ::Config.instance.oauth_mgmt_base_url = nil
      end
    end

    it "redirects to login on state mismatch" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = "https://localhost:15672"

        headers = ::HTTP::Headers{
          "Cookie" => "oauth_state=correct_state:verifier123",
        }
        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=wrong_state&code=authcode"),
          headers: headers
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("State%20mismatch")
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
        LavinMQ::Config.instance.oauth_mgmt_base_url = nil
      end
    end

    it "redirects to login when code query parameter is missing" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = "https://localhost:15672"

        headers = ::HTTP::Headers{
          "Cookie" => "oauth_state=mystate:verifier123",
        }
        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=mystate"),
          headers: headers
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("Missing%20authorization%20code")
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
        LavinMQ::Config.instance.oauth_mgmt_base_url = nil
      end
    end
  end

  describe "AuthHandler JWT cookie" do
    it "rejects a fake JWT in the m cookie with 401" do
      with_http_server do |http, _|
        headers = ::HTTP::Headers{
          "Cookie" => "m=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fakepayload.fakesignature",
        }
        response = ::HTTP::Client.get(http.test_uri("/api/whoami"), headers: headers)
        response.status_code.should eq 401
      end
    end

    it "clears a stale JWT cookie on rejection" do
      with_http_server do |http, _|
        headers = ::HTTP::Headers{
          "Cookie" => "m=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fakepayload.fakesignature",
        }
        response = ::HTTP::Client.get(http.test_uri("/api/whoami"), headers: headers)
        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        cleared = set_cookies.any? { |c| c.starts_with?("m=;") && c.downcase.includes?("max-age=0") }
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
      with_http_server do |http, _|
        response = ::HTTP::Client.get(http.test_uri("/oauth/enabled"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        JSON.parse(response.body)["enabled"].as_bool.should be_false
      end
    end

    it "returns enabled=true when OAuth is configured" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = "https://localhost:15672"

        response = ::HTTP::Client.get(http.test_uri("/oauth/enabled"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        JSON.parse(response.body)["enabled"].as_bool.should be_true
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
        LavinMQ::Config.instance.oauth_mgmt_base_url = nil
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
        with_http_server do |http, s|
          LavinMQ::Config.instance.oauth_client_id = "test-client"
          LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
          LavinMQ::Config.instance.oauth_mgmt_base_url = "http://localhost:15672"

          oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
            issuer: "https://idp.example.com",
            jwks_uri: "http://#{idp_addr}/jwks",
            token_endpoint: "http://#{idp_addr}/token")
          install_oauth_fetcher(s, oidc)

          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)
          response.status_code.should eq 302
          response.headers["Location"].should start_with("/login?error=")
        ensure
          LavinMQ::Config.instance.oauth_client_id = nil
          LavinMQ::Config.instance.oauth_issuer_url = nil
          LavinMQ::Config.instance.oauth_mgmt_base_url = nil
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
        with_http_server do |http, s|
          LavinMQ::Config.instance.oauth_client_id = "test-client"
          LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
          LavinMQ::Config.instance.oauth_mgmt_base_url = "http://localhost:15672"

          oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
            issuer: "https://idp.example.com",
            jwks_uri: "http://#{idp_addr}/jwks",
            token_endpoint: "http://#{idp_addr}/token")
          install_oauth_fetcher(s, oidc)

          headers = ::HTTP::Headers{
            "Cookie" => "oauth_state=st8:ver1fier",
          }
          response = ::HTTP::Client.get(
            http.test_uri("/oauth/callback?state=st8&code=authcode"),
            headers: headers)
          response.status_code.should eq 302
          response.headers["Location"].should contain("Token%20validation%20failed")
        ensure
          LavinMQ::Config.instance.oauth_client_id = nil
          LavinMQ::Config.instance.oauth_issuer_url = nil
          LavinMQ::Config.instance.oauth_mgmt_base_url = nil
        end
      ensure
        idp_server.close
      end
    end
  end

  describe "OAuthController /oauth/callback success cookies" do
    it "sets m (HttpOnly, SameSite=Strict) and oauth_user (SameSite=Strict, not HttpOnly) with Secure" do
      idp_server, idp_addr = start_stub_idp do |ctx|
        ctx.response.content_type = "application/json"
        ctx.response.print %({"access_token":"dummy","expires_in":3600})
      end
      begin
        with_http_server do |http, s|
          LavinMQ::Config.instance.oauth_client_id = "test-client"
          LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
          LavinMQ::Config.instance.oauth_mgmt_base_url = "http://localhost:15672"

          oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
            issuer: "https://idp.example.com",
            jwks_uri: "http://#{idp_addr}/jwks",
            token_endpoint: "http://#{idp_addr}/token")
          install_oauth_fetcher(s, oidc)

          fake = FakeBaseUser.new("alice@example.com")
          chain = s.authenticator.as(LavinMQ::Auth::Chain)
          original_backends = chain.backends
          chain.backends = [StubOAuthUserAuthenticator.new(fake)] of LavinMQ::Auth::Authenticator

          begin
            headers = ::HTTP::Headers{
              "Cookie" => "oauth_state=st8:ver1fier",
            }
            response = ::HTTP::Client.get(
              http.test_uri("/oauth/callback?state=st8&code=authcode"),
              headers: headers)
            response.status_code.should eq 302
            response.headers["Location"].should eq("/")

            set_cookies = response.headers.get?("Set-Cookie") || [] of String
            m_cookie = set_cookies.find { |c| c.starts_with?("m=") && !c.starts_with?("m=;") }
            user_cookie = set_cookies.find(&.starts_with?("oauth_user="))

            m_cookie.should_not be_nil
            m_lower = m_cookie.not_nil!.downcase
            m_lower.should contain("secure")
            m_lower.should contain("httponly")
            m_lower.should contain("samesite=strict")

            user_cookie.should_not be_nil
            user_lower = user_cookie.not_nil!.downcase
            user_lower.should contain("secure")
            user_lower.should contain("samesite=strict")
            user_lower.should_not contain("httponly")
            user_cookie.not_nil!.should contain("alice%40example.com")
          ensure
            chain.backends = original_backends
          end
        ensure
          LavinMQ::Config.instance.oauth_client_id = nil
          LavinMQ::Config.instance.oauth_issuer_url = nil
          LavinMQ::Config.instance.oauth_mgmt_base_url = nil
        end
      ensure
        idp_server.close
      end
    end
  end

  describe "OAuthController /oauth/authorize cookie flags" do
    it "sets oauth_state with Secure, HttpOnly, SameSite=Lax" do
      with_http_server do |http, s|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")
        LavinMQ::Config.instance.oauth_mgmt_base_url = "http://localhost:15672"

        oidc = LavinMQ::Auth::JWT::JWKSFetcher::OIDCConfiguration.new(
          issuer: "https://idp.example.com",
          jwks_uri: "https://idp.example.com/jwks",
          authorization_endpoint: "https://idp.example.com/authorize",
          token_endpoint: "https://idp.example.com/token")
        install_oauth_fetcher(s, oidc)

        response = ::HTTP::Client.get(http.test_uri("/oauth/authorize"), headers: ::HTTP::Headers.new)
        response.status_code.should eq 200
        set_cookies = response.headers.get?("Set-Cookie") || [] of String
        state_cookie = set_cookies.find(&.starts_with?("oauth_state="))
        state_cookie.should_not be_nil
        state_cookie.not_nil!.downcase.should contain("secure")
        state_cookie.not_nil!.downcase.should contain("httponly")
        state_cookie.not_nil!.downcase.should contain("samesite=lax")
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
        LavinMQ::Config.instance.oauth_mgmt_base_url = nil
      end
    end
  end
end
