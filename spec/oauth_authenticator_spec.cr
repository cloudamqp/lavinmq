require "./spec_helper"

describe LavinMQ::Auth::OAuthAuthenticator do
  # Note: These tests focus on the prevalidation checks and error handling paths in the
  # OAuth authenticator that can be tested without a real JWKS endpoint. Full JWT validation
  # requires network access to fetch public keys, which would need integration tests with
  # a mock OAuth server.

  describe "#authenticate with invalid tokens" do
    it "rejects tokens that don't start with 'ey'" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      user = authenticator.authenticate("testuser", "not-a-jwt-token")
      user.should be_nil
    end

    it "rejects tokens with invalid JWT format (wrong number of parts)" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Only 2 parts instead of 3
      user = authenticator.authenticate("testuser", "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0")
      user.should be_nil
    end

    it "rejects tokens with more than 3 parts" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # 4 parts
      user = authenticator.authenticate("testuser", "eyA.eyB.eyC.eyD")
      user.should be_nil
    end

    it "rejects tokens with missing algorithm in header" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Create token with no "alg" field
      header = {"typ" => "JWT"}
      payload = {"sub" => "test"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature"
      user = authenticator.authenticate("testuser", token)
      user.should be_nil
    end

    it "rejects tokens with non-RS256 algorithm" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Create token with HS256 algorithm (should be rejected)
      header = {"alg" => "HS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature"
      user = authenticator.authenticate("testuser", token)
      user.should be_nil
    end

    it "rejects tokens with missing exp claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Create token without exp claim
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature"
      user = authenticator.authenticate("testuser", token)
      user.should be_nil
    end

    it "rejects expired tokens in prevalidation" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Create token that expired 1 hour ago
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix - 3600).to_i64, "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature"
      user = authenticator.authenticate("testuser", token)
      user.should be_nil
    end

    it "rejects token with None algorithm (security check)" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      # Try "none" algorithm attack
      header = {"alg" => "none", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64, "preferred_username" => "hacker"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}."
      user = authenticator.authenticate("testuser", token)
      user.should be_nil
    end
  end

  describe "configuration requirements" do
    it "accepts valid configuration" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]

      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with multiple preferred username claims" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["email", "preferred_username", "sub"]

      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with optional audience verification disabled" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_verify_aud = false

      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with custom scope prefix" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_scope_prefix = "mq."

      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with resource server ID" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_resource_server_id = "lavinmq-api"

      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end
  end
end
