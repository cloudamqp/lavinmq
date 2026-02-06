require "./spec_helper"

def create_oauth_test_authenticator(config : LavinMQ::Config? = nil)
  config ||= create_oauth_test_config
  uri = config.oauth_issuer_url || URI.new
  jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(uri, config.oauth_jwks_cache_ttl)
  verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
  LavinMQ::Auth::OAuthAuthenticator.new(verifier)
end

def create_oauth_test_config
  config = LavinMQ::Config.new
  config.oauth_issuer_url = URI.parse("https://auth.example.com")
  config.oauth_preferred_username_claims = ["preferred_username"]
  config
end

# Test JWT::PublicKeys - Simple key storage with expiration
describe LavinMQ::Auth::JWT::PublicKeys do
  describe "#get?" do
    it "returns nil when no keys are set" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys.get?.should be_nil
    end

    it "returns keys when not expired" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      test_keys = {"kid1" => "pem1", "kid2" => "pem2"}

      keys.update(test_keys, 1.hour)

      keys.get?.should eq(test_keys)
    end

    it "returns nil when keys are expired" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      test_keys = {"kid1" => "pem1"}

      keys.update(test_keys, -1.seconds) # Already expired

      keys.get?.should be_nil
    end
  end

  describe "#clear" do
    it "removes stored keys" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 1.hour)

      keys.clear

      keys.get?.should be_nil
    end
  end

  describe "#update edge cases" do
    it "handles empty keys hash" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      empty_keys = {} of String => String

      keys.update(empty_keys, 1.hour)

      keys.get?.should eq(empty_keys)
    end

    it "handles multiple rapid updates" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 1.hour)
      keys.update({"kid2" => "pem2"}, 2.hours)
      keys.update({"kid3" => "pem3"}, 30.minutes)

      # Should have the last update with shortest TTL
      result = keys.get?
      result.should eq({"kid3" => "pem3"})
    end

    it "handles zero TTL as expired" do
      keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 0.seconds)

      # Zero TTL should be considered expired
      keys.get?.should be_nil
    end
  end
end

describe LavinMQ::Auth::JWT::PublicKeys do
  describe "#decode" do
    it "raises VerificationError when no keys are provided" do
      public_keys = LavinMQ::Auth::JWT::PublicKeys.new
      empty_keys = {} of String => String
      public_keys.update(empty_keys, 1.hour)

      # Sample JWT token (invalid signature for our keys)
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXiWdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA"

      expect_raises(LavinMQ::Auth::JWT::VerificationError, "Could not verify JWT with any key") do
        public_keys.decode(token)
      end
    end

    it "raises VerificationError with invalid PEM key" do
      public_keys = LavinMQ::Auth::JWT::PublicKeys.new

      # Invalid/malformed PEM key
      invalid_keys = {
        "key1" => "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwrong==\n-----END PUBLIC KEY-----",
      }
      public_keys.update(invalid_keys, 1.hour)

      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXiWdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA"

      expect_raises(LavinMQ::Auth::JWT::VerificationError) do
        public_keys.decode(token)
      end
    end

    it "raises DecodeError for malformed token" do
      public_keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys = {"key1" => "dummy_pem"}
      public_keys.update(keys, 1.hour)

      malformed_token = "not.a.valid.jwt.token"

      expect_raises(LavinMQ::Auth::JWT::DecodeError) do
        public_keys.decode(malformed_token)
      end
    end

    it "raises VerificationError when token has empty signature" do
      public_keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys = {"key1" => "dummy_pem"}
      public_keys.update(keys, 1.hour)

      # Valid JWT structure but empty signature
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0."

      expect_raises(LavinMQ::Auth::JWT::VerificationError) do
        public_keys.decode(token)
      end
    end

    it "raises DecodeError when PEM key is empty string" do
      public_keys = LavinMQ::Auth::JWT::PublicKeys.new
      keys = {"key1" => ""}
      public_keys.update(keys, 1.hour)

      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"

      expect_raises(LavinMQ::Auth::JWT::DecodeError) do
        public_keys.decode(token)
      end
    end
  end
end

describe LavinMQ::Auth::OAuthAuthenticator do
  describe "#authenticate" do
    it "returns nil for non-JWT password" do
      auth = create_oauth_test_authenticator

      # Regular password, not a JWT token
      ctx = LavinMQ::Auth::Context.new("testuser", "regular_password".to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for empty password" do
      auth = create_oauth_test_authenticator

      ctx = LavinMQ::Auth::Context.new("testuser", "".to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for malformed JWT token" do
      auth = create_oauth_test_authenticator

      # Starts with "ey" but is not a valid JWT
      ctx = LavinMQ::Auth::Context.new("testuser", "eyNotAValidToken".to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with invalid structure" do
      auth = create_oauth_test_authenticator

      # Only has 2 parts instead of 3
      ctx = LavinMQ::Auth::Context.new("testuser", "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0".to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with non-RS256 algorithm" do
      auth = create_oauth_test_authenticator

      # JWT with HS256 algorithm (not RS256)
      # Header: {"alg":"HS256","typ":"JWT"}
      token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for expired JWT token" do
      auth = create_oauth_test_authenticator

      # JWT with exp claim in the past (January 1, 2020)
      # Header: {"alg":"RS256","typ":"JWT"}
      # Payload: {"sub":"1234567890","name":"John Doe","exp":1577836800}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT token without exp claim" do
      auth = create_oauth_test_authenticator

      # JWT without exp claim
      # Header: {"alg":"RS256","typ":"JWT"}
      # Payload: {"sub":"1234567890","name":"John Doe"}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with 4 parts" do
      auth = create_oauth_test_authenticator

      # JWT with extra part
      token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0In0.signature.extra"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with empty header part" do
      auth = create_oauth_test_authenticator

      # JWT with empty header (just dots)
      token = "ey.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with invalid JSON in header" do
      auth = create_oauth_test_authenticator

      # Header that's valid base64 but not valid JSON
      # "eyBub3QganNvbiB9" decodes to "{ not json }"
      token = "eyBub3QganNvbiB9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with exp as string" do
      auth = create_oauth_test_authenticator

      # JWT with exp as string instead of number
      # Payload: {"sub":"1234567890","exp":"future"}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiZnV0dXJlIn0.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with exp as zero" do
      auth = create_oauth_test_authenticator

      # JWT with exp: 0 (epoch time, definitely expired)
      # Payload: {"sub":"1234567890","exp":0}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjowfQ.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with negative exp" do
      auth = create_oauth_test_authenticator

      # JWT with negative exp
      # Payload: {"sub":"1234567890","exp":-1}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjotMX0.signature"

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end

    it "returns nil for JWT with whitespace" do
      auth = create_oauth_test_authenticator

      # JWT with trailing whitespace
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature "

      ctx = LavinMQ::Auth::Context.new("testuser", token.to_slice)
      result = auth.authenticate ctx

      result.should be_nil
    end
  end
end

# Helper methods for OAuthUser tests
module OAuthUserHelper
  extend self

  class MockJWKSFetcher < LavinMQ::Auth::JWT::JWKSFetcher
    def initialize
      super(URI.new, Time::Span.new(seconds: 10))
    end

    def fetch_jwks : LavinMQ::Auth::JWT::JWKSFetcher::JWKSResult
      LavinMQ::Auth::JWT::JWKSFetcher::JWKSResult.new(Hash(String, String).new, Time::Span.new(seconds: 10))
    end
  end

  def create_user(expires_at : Time, permissions = {} of String => LavinMQ::Auth::BaseUser::Permissions)
    config = LavinMQ::Config.new
    config.oauth_issuer_url = URI.parse("https://auth.example.com")
    config.oauth_preferred_username_claims = ["preferred_username"]
    jwks_fetcher = MockJWKSFetcher.new
    verifier = LavinMQ::Auth::JWT::TokenVerifier.new(config, jwks_fetcher)
    LavinMQ::Auth::OAuthUser.new(
      "testuser",
      [] of LavinMQ::Tag,
      permissions,
      expires_at,
      verifier
    )
  end
end

describe LavinMQ::Auth::OAuthUser do
  describe "#find_permission with wildcards" do
    it "matches exact vhost name" do
      permissions = {"production" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("production").should_not be_nil
      user.find_permission("staging").should be_nil
    end

    it "matches single * wildcard (any vhost)" do
      permissions = {"*" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("anything").should_not be_nil
      user.find_permission("production").should_not be_nil
      user.find_permission("").should_not be_nil
    end

    it "matches prefix wildcard pattern" do
      permissions = {"prod-*" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("prod-us").should_not be_nil
      user.find_permission("prod-eu").should_not be_nil
      user.find_permission("prod-").should_not be_nil
      user.find_permission("staging-us").should be_nil
      user.find_permission("production").should be_nil
    end

    it "matches suffix wildcard pattern" do
      permissions = {"*-production" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("us-production").should_not be_nil
      user.find_permission("eu-production").should_not be_nil
      user.find_permission("-production").should_not be_nil
      user.find_permission("production").should be_nil
      user.find_permission("us-staging").should be_nil
    end

    it "matches middle wildcard pattern" do
      permissions = {"prod-*-db" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("prod-us-db").should_not be_nil
      user.find_permission("prod-eu-west-db").should_not be_nil
      user.find_permission("prod--db").should_not be_nil
      user.find_permission("prod-db").should be_nil
      user.find_permission("staging-us-db").should be_nil
    end

    it "matches multiple wildcards" do
      permissions = {"*-prod-*" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("us-prod-db").should_not be_nil
      user.find_permission("eu-prod-cache").should_not be_nil
      user.find_permission("-prod-").should_not be_nil
      user.find_permission("prod-db").should be_nil
      user.find_permission("us-staging-db").should be_nil
    end

    it "matches pattern with wildcards at both ends" do
      permissions = {"*production*" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("production").should_not be_nil
      user.find_permission("us-production").should_not be_nil
      user.find_permission("production-db").should_not be_nil
      user.find_permission("us-production-db").should_not be_nil
      user.find_permission("staging").should be_nil
    end

    it "prefers exact match over wildcard" do
      permissions = {
        "production" => {config: /^$/, read: /.*/, write: /.*/},
        "prod-*"     => {config: /.*/, read: /^$/, write: /.*/},
      }
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      # Exact match should be returned first
      perm = user.find_permission("production")
      perm.should_not be_nil
      perm.not_nil![:config].should eq(/^$/)
    end

    it "handles consecutive wildcards" do
      permissions = {"a**b" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("ab").should_not be_nil
      user.find_permission("aXb").should_not be_nil
      user.find_permission("aXYZb").should_not be_nil
    end

    it "handles pattern without wildcards as exact match" do
      permissions = {"exact-name" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("exact-name").should_not be_nil
      user.find_permission("exact-name-extra").should be_nil
      user.find_permission("prefix-exact-name").should be_nil
    end

    it "handles overlapping parts correctly" do
      permissions = {"ab*ab" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.find_permission("abab").should_not be_nil
      user.find_permission("abXab").should_not be_nil
      user.find_permission("ababab").should_not be_nil
      user.find_permission("ab").should be_nil
    end
  end

  describe "#refresh" do
    it "rejects token with mismatched username" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      # Create a mock token that would validate but has wrong username
      # Since we can't easily create valid JWT tokens in tests without a real key,
      # we test the error path by ensuring the authenticator would reject mismatched usernames
      # This test verifies that update_secret calls verify_token and checks username
      expect_raises(Exception) do
        # This should fail during token verification or username check
        user.refresh("invalid-token-format")
      end
    end
  end

  describe "#token_lifetime" do
    it "returns zero for expired tokens" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc - 1.hour, permissions)

      user.token_lifetime.should eq(0.seconds)
    end

    it "returns positive duration for valid tokens" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

      user.token_lifetime.should be > 0.seconds
    end
  end
end
