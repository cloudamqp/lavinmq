require "../spec_helper"
require "../../src/lavinmq/http/oauth2/pkce"

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

        response = ::HTTP::Client.get(
          http.test_uri("/oauth/callback?state=abc123&code=authcode"),
          headers: ::HTTP::Headers.new
        )
        response.status_code.should eq 302
        response.headers["Location"].should contain("Missing%20OAuth%20state")
      ensure
        LavinMQ::Config.instance.oauth_client_id = nil
        LavinMQ::Config.instance.oauth_issuer_url = nil
      end
    end

    it "redirects to login on state mismatch" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")

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
      end
    end

    it "redirects to login when code query parameter is missing" do
      with_http_server do |http, _|
        LavinMQ::Config.instance.oauth_client_id = "test-client"
        LavinMQ::Config.instance.oauth_issuer_url = URI.parse("https://idp.example.com")

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
end
