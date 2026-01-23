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
    additional_scopes_key : String? = nil,
  ) : LavinMQ::Auth::JWT::TokenVerifier
    config = LavinMQ::Config.new
    config.oauth_issuer_url = URI.parse(issuer_url)
    config.oauth_preferred_username_claims = preferred_username_claims
    config.oauth_verify_aud = verify_aud
    config.oauth_audience = audience
    config.oauth_resource_server_id = resource_server_id
    config.oauth_scope_prefix = scope_prefix
    config.oauth_additional_scopes_key = additional_scopes_key

    jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
    LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
  end

  # Create a token parser for testing claim extraction
  def create_token_parser(
    issuer_url = "https://auth.example.com",
    preferred_username_claims = ["preferred_username"],
    verify_aud = true,
    audience : String? = nil,
    resource_server_id : String? = nil,
    scope_prefix : String? = nil,
    additional_scopes_key : String? = nil,
  ) : LavinMQ::Auth::JWT::TokenParser
    config = LavinMQ::Config.new
    config.oauth_issuer_url = URI.parse(issuer_url)
    config.oauth_preferred_username_claims = preferred_username_claims
    config.oauth_verify_aud = verify_aud
    config.oauth_audience = audience
    config.oauth_resource_server_id = resource_server_id
    config.oauth_scope_prefix = scope_prefix
    config.oauth_additional_scopes_key = additional_scopes_key
    LavinMQ::Auth::JWT::TokenParser.new(config)
  end

  # Create a mock Token object for testing TokenParser
  def create_mock_token(payload : JSON::Any) : LavinMQ::Auth::JWT::Token
    LavinMQ::Auth::JWT::Token.new(JSON::Any.new({} of String => JSON::Any), payload, Bytes.new(0))
  end

  # Create a testable verifier that exposes protected methods for testing
  def create_testable_verifier(
    issuer_url = "https://auth.example.com",
    preferred_username_claims = ["preferred_username"],
    verify_aud = false,
    audience : String? = nil,
    resource_server_id : String? = nil,
    scope_prefix : String? = nil,
    additional_scopes_key : String? = nil,
  ) : TestableTokenVerifier
    config = LavinMQ::Config.new
    config.oauth_issuer_url = URI.parse(issuer_url)
    config.oauth_preferred_username_claims = preferred_username_claims
    config.oauth_verify_aud = verify_aud
    config.oauth_audience = audience
    config.oauth_resource_server_id = resource_server_id
    config.oauth_scope_prefix = scope_prefix
    config.oauth_additional_scopes_key = additional_scopes_key

    jwks_fetcher = MockJWKSFetcher.new
    TestableTokenVerifier.new(config, jwks_fetcher)
  end
end

# Testable subclass that exposes protected methods for unit testing
class TestableTokenVerifier < LavinMQ::Auth::JWT::TokenVerifier
  def test_validate_issuer(payload)
    validate_issuer(payload)
  end

  def test_validate_and_extract_claims(payload) : LavinMQ::Auth::JWT::TokenClaim
    validate_issuer(payload)
    test_validate_audience(payload) if @config.oauth_verify_aud?
    parser = LavinMQ::Auth::JWT::TokenParser.new(@config)
    token = LavinMQ::Auth::JWT::Token.new(JSON::Any.new({} of String => JSON::Any), payload, Bytes.new(0))
    parser.parse(token)
  end

  # Duplicates validate_audience logic since it's private in parent class
  private def test_validate_audience(payload)
    aud = payload["aud"]?
    raise LavinMQ::Auth::JWT::VerificationError.new("Missing aud claim in token") unless aud

    audiences = case aud
                when .as_a? then aud.as_a.map(&.as_s)
                when .as_s? then [aud.as_s]
                else
                  raise LavinMQ::Auth::JWT::DecodeError.new("Invalid aud claim format")
                end

    expected = @config.oauth_audience.nil? ? @config.oauth_resource_server_id : @config.oauth_audience

    if expected.nil?
      raise LavinMQ::Auth::JWT::DecodeError.new("Token contains audience claim but no expected audience is configured")
    end

    unless audiences.includes?(expected)
      raise LavinMQ::Auth::JWT::VerificationError.new("Token audience does not match expected value")
    end
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
      it "rejects tokens with missing algorithm in header" do
        verifier = JWTTestHelper.create_verifier
        token = JWTTestHelper.build_unsigned_token({"typ" => "JWT"}, JWTTestHelper.valid_payload)
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Missing algorithm in header/) do
          verifier.verify_token(token)
        end
      end

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

  describe "#validate_and_extract_claims" do
    describe "#validate_issuer" do
      it "accepts matching issuer" do
        parser = JWTTestHelper.create_token_parser
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser"}))
        token = JWTTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.username.should eq("testuser")
      end

      it "accepts matching issuer with trailing slash difference" do
        verifier = JWTTestHelper.create_testable_verifier(issuer_url: "https://auth.example.com/")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end

      it "rejects missing issuer" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser"}))
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /Missing or invalid iss claim/) do
          verifier.test_validate_issuer(payload)
        end
      end

      it "rejects mismatched issuer" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://evil.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser"}))
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token issuer does not match/) do
          verifier.test_validate_issuer(payload)
        end
      end
    end

    describe "#validate_audience" do
      it "rejects tokens without audience claim" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true, audience: "my-api")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser"}))
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /Missing aud claim/) do
          verifier.test_validate_and_extract_claims(payload)
        end
      end

      it "accepts matching string audience" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true, audience: "my-api")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": "my-api"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end

      it "accepts matching audience in array" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true, audience: "my-api")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": ["other-api", "my-api"]}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end

      it "uses resource_server_id as audience when oauth_audience is nil" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true, resource_server_id: "lavinmq")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": "lavinmq"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end

      it "rejects audience when no expected audience is configured" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true)
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": "some-api"}))
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /no expected audience is configured/) do
          verifier.test_validate_and_extract_claims(payload)
        end
      end

      it "rejects mismatched audience" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: true, audience: "my-api")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": "other-api"}))
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /Token audience does not match/) do
          verifier.test_validate_and_extract_claims(payload)
        end
      end

      it "skips audience validation when oauth_verify_aud is false" do
        verifier = JWTTestHelper.create_testable_verifier(verify_aud: false, audience: "my-api")
        # Mismatched audience should be accepted when verification is disabled
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "aud": "wrong-api"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end
    end

    describe "#extract_username" do
      it "extracts username from first matching claim" do
        verifier = JWTTestHelper.create_testable_verifier(preferred_username_claims: ["email", "preferred_username", "sub"])
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "sub": "12345"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("testuser")
      end

      it "falls back to subsequent claims" do
        verifier = JWTTestHelper.create_testable_verifier(preferred_username_claims: ["email", "preferred_username", "sub"])
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "sub": "12345"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.username.should eq("12345")
      end

      it "raises when no username claim is found" do
        verifier = JWTTestHelper.create_testable_verifier(preferred_username_claims: ["email", "preferred_username"])
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "sub": "12345"}))
        expect_raises(Exception, /No username found/) do
          verifier.test_validate_and_extract_claims(payload)
        end
      end
    end

    describe "#parse_roles" do
      it "extracts tags from scope" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "tag:administrator tag:monitoring"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.tags.should contain(LavinMQ::Tag::Monitoring)
      end

      it "extracts permissions from scope" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "read:*/* write:myvhost/myqueue"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.permissions["*"][:read].should eq(/.*/)
        claims.permissions["myvhost"][:write].should eq(/^myqueue$/)
      end

      it "extracts configure permission" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "configure:*/myqueue"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.permissions["*"][:config].should eq(/^myqueue$/)
      end

      it "extracts roles from resource_access" do
        verifier = JWTTestHelper.create_testable_verifier(resource_server_id: "lavinmq")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "resource_access": {"lavinmq": {"roles": ["lavinmq.tag:administrator", "lavinmq.read:*/*"]}}}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.permissions["*"][:read].should eq(/.*/)
      end

      it "filters scopes by prefix" do
        verifier = JWTTestHelper.create_testable_verifier(scope_prefix: "rabbitmq.")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "rabbitmq.tag:administrator other.tag:management"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.tags.should_not contain(LavinMQ::Tag::Management)
      end

      it "extracts scopes from additional_scopes_key (string)" do
        verifier = JWTTestHelper.create_testable_verifier(additional_scopes_key: "custom_permissions")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "custom_permissions": "tag:administrator read:*/*"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.permissions["*"][:read].should eq(/.*/)
      end

      it "handles nested additional_scopes_key with hash" do
        verifier = JWTTestHelper.create_testable_verifier(additional_scopes_key: "permissions", resource_server_id: "lavinmq")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "permissions": {"lavinmq": "lavinmq.tag:administrator"}}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
      end

      it "handles array in additional_scopes_key" do
        verifier = JWTTestHelper.create_testable_verifier(additional_scopes_key: "roles")
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "roles": ["tag:administrator", "tag:monitoring"]}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.tags.should contain(LavinMQ::Tag::Monitoring)
      end

      it "ignores invalid permission format (not 2 or 3 parts)" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "invalid read:*/*"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.permissions["*"][:read].should eq(/.*/)
        # "invalid" should be skipped (no colon separator)
      end

      it "ignores invalid regex patterns" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "read:*/[invalid read:*/valid"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.permissions["*"][:read].should eq(/^valid$/)
      end

      it "ignores unknown permission types" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "delete:*/* read:*/*"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.permissions["*"][:read].should eq(/.*/)
        # "delete" permission type should be ignored
      end

      it "ignores unknown tags" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{RoughTime.utc.to_unix + 3600}, "preferred_username": "testuser", "scope": "tag:superuser tag:administrator"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.tags.should contain(LavinMQ::Tag::Administrator)
        claims.tags.size.should eq(1) # superuser is not a valid tag
      end
    end

    describe "expiration extraction" do
      it "extracts expiration time from token" do
        verifier = JWTTestHelper.create_testable_verifier
        exp_time = RoughTime.utc.to_unix + 7200
        payload = JSON.parse(%({"iss": "https://auth.example.com", "exp": #{exp_time}, "preferred_username": "testuser"}))
        claims = verifier.test_validate_and_extract_claims(payload)
        claims.expires_at.to_unix.should eq(exp_time)
      end

      it "raises when exp is missing in validate_and_extract_claims" do
        verifier = JWTTestHelper.create_testable_verifier
        payload = JSON.parse(%({"iss": "https://auth.example.com", "preferred_username": "testuser"}))
        expect_raises(LavinMQ::Auth::JWT::DecodeError, /No expiration time found/) do
          verifier.test_validate_and_extract_claims(payload)
        end
      end
    end
  end
end
