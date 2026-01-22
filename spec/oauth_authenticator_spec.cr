require "./spec_helper"

def create_test_authenticator(config : LavinMQ::Config? = nil)
  config ||= create_test_config
  # public_keys = LavinMQ::Auth::PublicKeys.new
  jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
  verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
  LavinMQ::Auth::OAuthAuthenticator.new(verifier)
end

def create_test_config
  config = LavinMQ::Config.new
  config.oauth_issuer_url = URI.parse("https://auth.example.com")
  config.oauth_preferred_username_claims = ["preferred_username"]
  config
end

describe LavinMQ::Auth::OAuthAuthenticator do
  # Note: These tests focus on the prevalidation checks and error handling paths in the
  # OAuth authenticator that can be tested without a real JWKS endpoint. Full JWT validation
  # requires network access to fetch public keys, which would need integration tests with
  # a mock OAuth server.

  describe "#authenticate with invalid tokens" do
    it "rejects tokens that don't start with 'ey'" do
      authenticator = create_test_authenticator

      ctx = LavinMQ::Auth::Context.new("testuser", "not-a-jwt-token".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with invalid JWT format (wrong number of parts)" do
      authenticator = create_test_authenticator

      # Only 2 parts instead of 3
      ctx = LavinMQ::Auth::Context.new("testuser", "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with more than 3 parts" do
      authenticator = create_test_authenticator

      # 4 parts
      ctx = LavinMQ::Auth::Context.new("testuser", "eyA.eyB.eyC.eyD".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with missing algorithm in header" do
      authenticator = create_test_authenticator

      # Create token with no "alg" field
      header = {"typ" => "JWT"}
      payload = {"sub" => "test"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with non-RS256 algorithm" do
      authenticator = create_test_authenticator

      # Create token with HS256 algorithm (should be rejected)
      header = {"alg" => "HS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with missing exp claim" do
      authenticator = create_test_authenticator

      # Create token without exp claim
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects expired tokens in prevalidation" do
      authenticator = create_test_authenticator

      # Create token that expired 1 hour ago
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix - 3600).to_i64, "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects token with None algorithm (security check)" do
      authenticator = create_test_authenticator

      # Try "none" algorithm attack
      header = {"alg" => "none", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64, "preferred_username" => "hacker"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end
  end

  describe "configuration requirements" do
    it "accepts valid configuration" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]

      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with multiple preferred username claims" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["email", "preferred_username", "sub"]

      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with optional audience verification disabled" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_verify_aud = false

      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with custom scope prefix" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_scope_prefix = "mq."

      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with resource server ID" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_resource_server_id = "lavinmq-api"

      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end
  end
end
