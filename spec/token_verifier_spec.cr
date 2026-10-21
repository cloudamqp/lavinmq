require "./spec_helper"
require "../src/lavinmq/auth/jwt/token_verifier"

# Mock JWKSFetcher that returns pre-configured keys without HTTP requests
class MockJWKSFetcher < LavinMQ::Auth::JWT::JWKSFetcher
  @keys = Hash(String, String).new
  @ttl : Time::Span = 10.seconds

  def initialize(@keys : Hash(String, String) = {} of String => String, @ttl : Time::Span = 1.hour)
    super(URI.parse("https://mock.example.com"), @ttl)
  end

  def fetch_jwks : JWKSResult
    JWKSResult.new(@keys, @ttl)
  end
end

# JWKSFetcher whose fetch blocks until released. Not a MockJWKSFetcher
# subclass: that name is also defined in auth_spec.cr, so its constructors
# depend on the order the spec files are compiled in.
class BlockingJWKSFetcher < LavinMQ::Auth::JWT::JWKSFetcher
  getter fetching = Channel(Nil).new
  getter release = Channel(Nil).new

  def initialize
    super(URI.parse("https://mock.example.com"), 1.hour)
  end

  def fetch_jwks : JWKSResult
    @fetching.send nil
    @release.receive
    JWKSResult.new({"k1" => "not-a-pem"}, 1.hour)
  end
end

# Helper module to build JWT tokens for testing
module JWTTestHelper
  extend self

  # Create base64url-encoded string
  def base64url_encode(data : String) : String
    Base64.strict_encode(data).tr("+/", "-_").gsub(/=+$/, "")
  end

  # Build an unsigned JWT token (for prevalidation tests)
  def build_unsigned_token(header : Hash, payload : Hash) : String
    header_b64 = base64url_encode(header.to_json)
    payload_b64 = base64url_encode(payload.to_json)
    "#{header_b64}.#{payload_b64}.fake_signature"
  end

  # Create a valid base payload with required fields
  def valid_payload(overrides = {} of String => JSON::Any::Type) : Hash(String, JSON::Any::Type)
    base = {
      "iss"                => "https://auth.example.com",
      "exp"                => (RoughTime.utc.to_unix + 3600).to_i64,
      "iat"                => (RoughTime.utc.to_unix - 60).to_i64,
      "preferred_username" => "testuser",
      "aud"                => "test-audience",
    } of String => JSON::Any::Type
    base.merge(overrides)
  end

  def valid_header : Hash(String, String)
    {"alg" => "RS256", "typ" => "JWT"}
  end

  # Create a verifier
  def create_verifier(
    issuer_url = "https://auth.example.com",
    preferred_username_claims = ["preferred_username"],
    verify_aud = true,
    audience : String? = nil,
    resource_server_id : String? = nil,
    scope_prefix : String? = nil,
    additional_scopes_keys = [] of String,
  ) : LavinMQ::Auth::JWT::TokenVerifier
    provider = LavinMQ::OAuthProviderConfig.new(
      issuer_url: URI.parse(issuer_url),
      preferred_username_claims: preferred_username_claims,
      verify_aud: verify_aud,
      audience: audience,
      resource_server_id: resource_server_id,
      scope_prefix: scope_prefix,
      additional_scopes_keys: additional_scopes_keys)

    jwks_fetcher = MockJWKSFetcher.new
    LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
  end

  # Create a mock Token object for testing TokenParser
  def create_mock_token(payload : LavinMQ::Auth::JWT::Payload) : LavinMQ::Auth::JWT::Token
    header = LavinMQ::Auth::JWT::Header.new(alg: "RS256", typ: "JWT")
    LavinMQ::Auth::JWT::Token.new(header, payload, Bytes.new(0))
  end

  # Create a testable verifier that exposes protected methods for testing
  def create_testable_verifier(
    issuer_url = "https://auth.example.com",
    preferred_username_claims = ["preferred_username"],
    verify_aud = false,
    audience : String? = nil,
    resource_server_id : String? = nil,
    scope_prefix : String? = nil,
    additional_scopes_keys = [] of String,
  ) : TestableTokenVerifier
    provider = LavinMQ::OAuthProviderConfig.new(
      issuer_url: URI.parse(issuer_url),
      preferred_username_claims: preferred_username_claims,
      verify_aud: verify_aud,
      audience: audience,
      resource_server_id: resource_server_id,
      scope_prefix: scope_prefix,
      additional_scopes_keys: additional_scopes_keys)

    jwks_fetcher = MockJWKSFetcher.new
    TestableTokenVerifier.new(provider, jwks_fetcher)
  end
end

# Testable subclass that exposes protected methods for unit testing
class TestableTokenVerifier < LavinMQ::Auth::JWT::TokenVerifier
  def test_validate_issuer(payload : LavinMQ::Auth::JWT::Payload)
    validate_issuer(payload)
  end

  def test_validate_audience(payload : LavinMQ::Auth::JWT::Payload)
    validate_audience(payload)
  end

  def test_parse(token : LavinMQ::Auth::JWT::Token) : LavinMQ::Auth::JWT::TokenClaim
    LavinMQ::Auth::JWT::TokenParser.new(provider).parse(token)
  end
end

describe LavinMQ::Auth::JWT::TokenVerifier do
  describe "#prevalidate_token (via verify_token)" do
    describe "token format validation" do
      it "rejects tokens that don't start with 'ey'" do
        verifier = JWTTestHelper.create_verifier
        expect_raises(LavinMQ::Auth::JWT::PasswordFormatError) do
          verifier.verify_token("not-a-jwt-token")
        end
      end

      it "rejects tokens with less than 3 parts" do
        verifier = JWTTestHelper.create_verifier
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Invalid JWT format/) do
          verifier.verify_token("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0")
        end
      end

      it "rejects tokens with more than 3 parts" do
        verifier = JWTTestHelper.create_verifier
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Invalid JWT format/) do
          verifier.verify_token("eyA.eyB.eyC.eyD")
        end
      end

      it "rejects tokens with invalid base64url encoding" do
        verifier = JWTTestHelper.create_verifier
        # base64.size % 4 == 1 is invalid
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Invalid base64url encoding/) do
          verifier.verify_token("eyJhb.eyJzdWIiOiIxMjM0NTY3ODkwIn0.fake")
        end
      end
    end

    describe "algorithm validation" do
      it "rejects tokens with HS256 algorithm" do
        verifier = JWTTestHelper.create_verifier
        token = JWTTestHelper.build_unsigned_token({"alg" => "HS256", "typ" => "JWT"}, JWTTestHelper.valid_payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Expected RS256, got HS256/) do
          verifier.verify_token(token)
        end
      end

      it "rejects tokens with 'none' algorithm (algorithm confusion attack)" do
        verifier = JWTTestHelper.create_verifier
        header_b64 = JWTTestHelper.base64url_encode({"alg" => "none", "typ" => "JWT"}.to_json)
        payload_b64 = JWTTestHelper.base64url_encode(JWTTestHelper.valid_payload.to_json)
        token = "#{header_b64}.#{payload_b64}."
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Expected RS256, got none/) do
          verifier.verify_token(token)
        end
      end

      it "rejects tokens with RS384 algorithm" do
        verifier = JWTTestHelper.create_verifier
        token = JWTTestHelper.build_unsigned_token({"alg" => "RS384", "typ" => "JWT"}, JWTTestHelper.valid_payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Expected RS256, got RS384/) do
          verifier.verify_token(token)
        end
      end
    end

    describe "exp claim validation" do
      it "rejects tokens without exp claim" do
        verifier = JWTTestHelper.create_verifier
        payload = JWTTestHelper.valid_payload
        payload.delete("exp")
        token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Missing exp claim/) do
          verifier.verify_token(token)
        end
      end

      it "rejects expired tokens" do
        verifier = JWTTestHelper.create_verifier
        payload = JWTTestHelper.valid_payload({"exp" => (RoughTime.utc.to_unix - 3600).to_i64})
        token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, payload)
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token has expired/) do
          verifier.verify_token(token)
        end
      end

      it "rejects tokens expiring exactly now" do
        verifier = JWTTestHelper.create_verifier
        payload = JWTTestHelper.valid_payload({"exp" => RoughTime.utc.to_unix})
        token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, payload)
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token has expired/) do
          verifier.verify_token(token)
        end
      end
    end

    describe "iat claim validation" do
      it "rejects tokens with iat in the future" do
        verifier = JWTTestHelper.create_verifier
        payload = JWTTestHelper.valid_payload({"iat" => (RoughTime.utc.to_unix + 3600).to_i64})
        token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Token issued in the future/) do
          verifier.verify_token(token)
        end
      end
    end

    describe "nbf claim validation (RFC 7519 Section 4.1.5)" do
      it "rejects tokens with nbf in the future" do
        verifier = JWTTestHelper.create_verifier
        payload = JWTTestHelper.valid_payload({"nbf" => (RoughTime.utc.to_unix + 3600).to_i64})
        token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Token not yet valid/) do
          verifier.verify_token(token)
        end
      end
    end
  end

  describe "#validate_issuer" do
    it "accepts matching issuer with trailing slash difference" do
      verifier = JWTTestHelper.create_testable_verifier(issuer_url: "https://auth.example.com/")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600)
      payload["preferred_username"] = JSON::Any.new("testuser")
      verifier.test_validate_issuer(payload)
    end

    it "rejects missing issuer" do
      verifier = JWTTestHelper.create_testable_verifier
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600)
      payload["preferred_username"] = JSON::Any.new("testuser")
      expect_raises(LavinMQ::Auth::JWT::DecodeError, /Missing or invalid iss claim/) do
        verifier.test_validate_issuer(payload)
      end
    end

    it "rejects mismatched issuer" do
      verifier = JWTTestHelper.create_testable_verifier
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://evil.example.com", exp: RoughTime.utc.to_unix + 3600)
      payload["preferred_username"] = JSON::Any.new("testuser")
      expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token issuer does not match/) do
        verifier.test_validate_issuer(payload)
      end
    end

    it "can't be reloaded with another issuer" do
      verifier = JWTTestHelper.create_testable_verifier
      expect_raises(ArgumentError) do
        verifier.reload(LavinMQ::OAuthProviderConfig.new(URI.parse("https://other.example.com")))
      end
      verifier.issuer.should eq "https://auth.example.com"
    end
  end

  describe "#validate_audience" do
    it "rejects tokens without audience claim" do
      verifier = JWTTestHelper.create_testable_verifier(audience: "my-api")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600)
      payload["preferred_username"] = JSON::Any.new("testuser")
      # Should not raise - no aud in token means nothing to validate
      expect_raises(LavinMQ::Auth::JWT::VerificationError, /Missing aud claim in token/) do
        verifier.test_validate_audience(payload)
      end
    end

    it "accepts matching string audience" do
      verifier = JWTTestHelper.create_testable_verifier(audience: "my-api")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: "my-api")
      payload["preferred_username"] = JSON::Any.new("testuser")
      verifier.test_validate_audience(payload)
      # claims.username.should eq("testuser")
    end

    it "accepts matching audience in array" do
      verifier = JWTTestHelper.create_testable_verifier(audience: "my-api")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: ["other-api", "my-api"])
      payload["preferred_username"] = JSON::Any.new("testuser")
      verifier.test_validate_audience(payload)
    end

    it "uses resource_server_id as audience when oauth_audience is nil" do
      verifier = JWTTestHelper.create_testable_verifier(resource_server_id: "lavinmq")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: "lavinmq")
      payload["preferred_username"] = JSON::Any.new("testuser")
      verifier.test_validate_audience(payload)
    end

    it "rejects audience when no expected audience is configured" do
      verifier = JWTTestHelper.create_testable_verifier
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: "some-api")
      payload["preferred_username"] = JSON::Any.new("testuser")
      expect_raises(LavinMQ::Auth::JWT::DecodeError, /no expected audience is configured/) do
        verifier.test_validate_audience(payload)
      end
    end

    it "rejects mismatched audience" do
      verifier = JWTTestHelper.create_testable_verifier(audience: "my-api")
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: "other-api")
      payload["preferred_username"] = JSON::Any.new("testuser")
      expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token audience does not match/) do
        verifier.test_validate_audience(payload)
      end
    end
  end

  describe "config snapshot" do
    it "isn't affected when Config#reload changes the config in place" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_audience = "my-api"
      # Same issuer as the config, so #reload keeps this fetcher. It's never
      # started and the spec never fetches keys, so it makes no requests.
      fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(URI.parse("https://auth.example.com"), 1.hour)
      verifier = TestableTokenVerifier.new(config.oauth_providers.first, fetcher)
      payload = LavinMQ::Auth::JWT::Payload.new(iss: "https://auth.example.com", exp: RoughTime.utc.to_unix + 3600, aud: "my-api")
      payload["preferred_username"] = JSON::Any.new("testuser")
      token = JWTTestHelper.create_mock_token(payload)

      # Config#reload overwrites the fields of the live Config
      config.oauth_audience = "other-api"
      config.oauth_preferred_username_claims = ["sub"]
      verifier.test_validate_audience(payload)
      verifier.test_parse(token).username.should eq "testuser"

      # The provider passed to #reload is detached too
      verifier.reload(config.oauth_providers.first)
      verifier.fetcher.should be fetcher
      config.oauth_audience = "my-api"
      expect_raises(LavinMQ::Auth::JWT::VerificationError, /audience does not match/) do
        verifier.test_validate_audience(payload)
      end
    end
  end

  describe "#disable" do
    it "rejects a token whose verification was waiting for keys" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      fetcher = BlockingJWKSFetcher.new
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config.oauth_providers.first, fetcher)
      token = JWTTestHelper.build_unsigned_token(JWTTestHelper.valid_header, JWTTestHelper.valid_payload)
      result = Channel(Exception?).new
      spawn do
        verifier.verify_token(token)
        result.send nil
      rescue ex
        result.send ex
      end
      fetcher.fetching.receive
      verifier.disable
      fetcher.release.send nil
      error = result.receive.should be_a(LavinMQ::Auth::JWT::VerificationError)
      error.message.should eq "OAuth authentication is disabled"
    end
  end
end
