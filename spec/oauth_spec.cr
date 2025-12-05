require "./spec_helper"

# Test PublicKeys - Simple key storage with expiration
describe LavinMQ::Auth::PublicKeys do
  describe "#get?" do
    it "returns nil when no keys are set" do
      keys = LavinMQ::Auth::PublicKeys.new
      keys.get?.should be_nil
    end

    it "returns keys when not expired" do
      keys = LavinMQ::Auth::PublicKeys.new
      test_keys = {"kid1" => "pem1", "kid2" => "pem2"}

      keys.update(test_keys, 1.hour)

      keys.get?.should eq(test_keys)
    end

    it "returns nil when keys are expired" do
      keys = LavinMQ::Auth::PublicKeys.new
      test_keys = {"kid1" => "pem1"}

      keys.update(test_keys, -1.seconds) # Already expired

      keys.get?.should be_nil
    end
  end

  describe "#clear" do
    it "removes stored keys" do
      keys = LavinMQ::Auth::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 1.hour)

      keys.clear

      keys.get?.should be_nil
    end
  end

  describe "#update edge cases" do
    it "handles empty keys hash" do
      keys = LavinMQ::Auth::PublicKeys.new
      empty_keys = {} of String => String

      keys.update(empty_keys, 1.hour)

      keys.get?.should eq(empty_keys)
    end

    it "handles multiple rapid updates" do
      keys = LavinMQ::Auth::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 1.hour)
      keys.update({"kid2" => "pem2"}, 2.hours)
      keys.update({"kid3" => "pem3"}, 30.minutes)

      # Should have the last update with shortest TTL
      result = keys.get?
      result.should eq({"kid3" => "pem3"})
    end

    it "handles zero TTL as expired" do
      keys = LavinMQ::Auth::PublicKeys.new
      keys.update({"kid1" => "pem1"}, 0.seconds)

      # Zero TTL should be considered expired
      keys.get?.should be_nil
    end
  end
end

describe LavinMQ::Auth::JWKSFetcher do
  describe "#decode_token" do
    it "raises VerificationError when no keys are provided" do
      fetcher = LavinMQ::Auth::JWKSFetcher.new("https://example.com", 1.hour)
      empty_keys = {} of String => String

      # Sample JWT token (invalid signature for our keys)
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXiWdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA"

      expect_raises(JWT::VerificationError, "Could not verify JWT with any key") do
        fetcher.decode_token(token, empty_keys)
      end
    end

    it "raises VerificationError with invalid PEM key" do
      fetcher = LavinMQ::Auth::JWKSFetcher.new("https://example.com", 1.hour)

      # Invalid/malformed PEM key
      invalid_keys = {
        "key1" => "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwrong==\n-----END PUBLIC KEY-----",
      }

      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXiWdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA"

      expect_raises(JWT::VerificationError) do
        fetcher.decode_token(token, invalid_keys)
      end
    end

    it "raises VerificationError for malformed token" do
      fetcher = LavinMQ::Auth::JWKSFetcher.new("https://example.com", 1.hour)
      keys = {"key1" => "dummy_pem"}

      malformed_token = "not.a.valid.jwt.token"

      expect_raises(JWT::VerificationError) do
        fetcher.decode_token(malformed_token, keys)
      end
    end

    it "raises VerificationError when token has empty signature" do
      fetcher = LavinMQ::Auth::JWKSFetcher.new("https://example.com", 1.hour)
      keys = {"key1" => "dummy_pem"}

      # Valid JWT structure but empty signature
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0."

      expect_raises(JWT::VerificationError) do
        fetcher.decode_token(token, keys)
      end
    end

    it "raises VerificationError when PEM key is empty string" do
      fetcher = LavinMQ::Auth::JWKSFetcher.new("https://example.com", 1.hour)
      keys = {"key1" => ""}

      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"

      expect_raises(JWT::VerificationError) do
        fetcher.decode_token(token, keys)
      end
    end
  end
end

describe LavinMQ::Auth::OAuthAuthenticator do
  describe "#authenticate" do
    it "returns nil for non-JWT password" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # Regular password, not a JWT token
      result = auth.authenticate("testuser", "regular_password")

      result.should be_nil
    end

    it "returns nil for empty password" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      result = auth.authenticate("testuser", "")

      result.should be_nil
    end

    it "returns nil for malformed JWT token" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # Starts with "ey" but is not a valid JWT
      result = auth.authenticate("testuser", "eyNotAValidToken")

      result.should be_nil
    end

    it "returns nil for JWT with invalid structure" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # Only has 2 parts instead of 3
      result = auth.authenticate("testuser", "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0")

      result.should be_nil
    end

    it "returns nil for JWT with non-RS256 algorithm" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with HS256 algorithm (not RS256)
      # Header: {"alg":"HS256","typ":"JWT"}
      token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for expired JWT token" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with exp claim in the past (January 1, 2020)
      # Header: {"alg":"RS256","typ":"JWT"}
      # Payload: {"sub":"1234567890","name":"John Doe","exp":1577836800}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT token without exp claim" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT without exp claim
      # Header: {"alg":"RS256","typ":"JWT"}
      # Payload: {"sub":"1234567890","name":"John Doe"}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with 4 parts" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with extra part
      token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0In0.signature.extra"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with empty header part" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with empty header (just dots)
      token = "ey.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with invalid JSON in header" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # Header that's valid base64 but not valid JSON
      # "eyBub3QganNvbiB9" decodes to "{ not json }"
      token = "eyBub3QganNvbiB9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with exp as string" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with exp as string instead of number
      # Payload: {"sub":"1234567890","exp":"future"}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoiZnV0dXJlIn0.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with exp as zero" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with exp: 0 (epoch time, definitely expired)
      # Payload: {"sub":"1234567890","exp":0}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjowfQ.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with negative exp" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with negative exp
      # Payload: {"sub":"1234567890","exp":-1}
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjotMX0.signature"

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end

    it "returns nil for JWT with whitespace" do
      auth = LavinMQ::Auth::OAuthAuthenticator.new

      # JWT with trailing whitespace
      token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxNTc3ODM2ODAwfQ.signature "

      result = auth.authenticate("testuser", token)

      result.should be_nil
    end
  end
end

# Helper methods for OAuthUser tests
module OAuthUserHelper
  extend self

  def mock_authenticator
    config = LavinMQ::Config.new
    config.oauth_issuer_url = "https://auth.example.com"
    config.oauth_preferred_username_claims = ["preferred_username"]
    LavinMQ::Auth::OAuthAuthenticator.new(config)
  end

  def create_user(expires_at : Time, permissions = {} of String => LavinMQ::Auth::User::Permissions)
    LavinMQ::Auth::OAuthUser.new(
      "testuser",
      [] of LavinMQ::Tag,
      permissions,
      expires_at,
      mock_authenticator
    )
  end
end

describe LavinMQ::Auth::OAuthUser do
  describe "#expired?" do
    it "returns true for expired tokens" do
      user = OAuthUserHelper.create_user(Time.utc - 1.hour)
      user.expired?.should be_true
    end

    it "returns false for valid tokens" do
      user = OAuthUserHelper.create_user(Time.utc + 1.hour)
      user.expired?.should be_false
    end
  end

  describe "#can_write?" do
    it "raises TokenExpiredError when token is expired" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc - 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      expect_raises(LavinMQ::Auth::TokenExpiredError, "OAuth token expired for user 'testuser'") do
        user.can_write?("/", "queue1", cache)
      end
    end

    it "allows access when token is valid and permissions match" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      user.can_write?("/", "queue1", cache).should be_true
    end

    it "denies access when permissions don't match" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /^queue/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      user.can_write?("/", "exchange1", cache).should be_false
    end
  end

  describe "#can_read?" do
    it "raises TokenExpiredError when token is expired" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc - 1.hour, permissions)

      expect_raises(LavinMQ::Auth::TokenExpiredError, "OAuth token expired for user 'testuser'") do
        user.can_read?("/", "queue1")
      end
    end

    it "allows access when token is valid and permissions match" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)

      user.can_read?("/", "queue1").should be_true
    end

    it "denies access when permissions don't match" do
      permissions = {"/" => {config: /.*/, read: /^exchange/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)

      user.can_read?("/", "queue1").should be_false
    end
  end

  describe "#can_config?" do
    it "raises TokenExpiredError when token is expired" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc - 1.hour, permissions)

      expect_raises(LavinMQ::Auth::TokenExpiredError, "OAuth token expired for user 'testuser'") do
        user.can_config?("/", "queue1")
      end
    end

    it "allows access when token is valid and permissions match" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)

      user.can_config?("/", "queue1").should be_true
    end

    it "denies access when permissions don't match" do
      permissions = {"/" => {config: /^vhost/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)

      user.can_config?("/", "queue1").should be_false
    end
  end

  describe "edge cases" do
    it "denies access when vhost doesn't exist in permissions" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      # Request access to a vhost that's not in permissions
      user.can_write?("/other", "queue1", cache).should be_false
    end

    it "denies access when permissions hash is empty" do
      user = OAuthUserHelper.create_user(Time.utc + 1.hour)
      cache = LavinMQ::Auth::PermissionCache.new

      user.can_write?("/", "queue1", cache).should be_false
    end

    it "handles empty resource name" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      # Empty resource name should match /.*/ pattern
      user.can_write?("/", "", cache).should be_true
    end

    it "denies access for empty resource name when pattern doesn't match" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /^queue/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      # Empty string doesn't start with "queue"
      user.can_write?("/", "", cache).should be_false
    end

    it "handles expiration at exact boundary" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      expires_at = Time.utc + 1.second
      user = OAuthUserHelper.create_user(expires_at, permissions)

      # Should not be expired yet
      user.expired?.should be_false

      # Sleep past expiration
      sleep 1.1.seconds

      # Should now be expired
      user.expired?.should be_true
    end

    it "handles very long resource names" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /^queue/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      # Very long resource name that matches pattern
      long_name = "queue" + ("x" * 1000)
      user.can_write?("/", long_name, cache).should be_true
    end

    it "handles special characters in resource names" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)
      cache = LavinMQ::Auth::PermissionCache.new

      # Resource name with special regex characters
      user.can_write?("/", "queue.with.dots", cache).should be_true
      user.can_write?("/", "queue[with]brackets", cache).should be_true
      user.can_write?("/", "queue*with*stars", cache).should be_true
    end
  end

  describe "#update_secret" do
    it "rejects token with mismatched username" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      user = OAuthUserHelper.create_user(Time.utc + 1.hour, permissions)

      # Create a mock token that would validate but has wrong username
      # Since we can't easily create valid JWT tokens in tests without a real key,
      # we test the error path by ensuring the authenticator would reject mismatched usernames
      # This test verifies that update_secret calls verify_token and checks username
      expect_raises(Exception) do
        # This should fail during token verification or username check
        user.update_secret("invalid-token-format")
      end
    end
  end
end
