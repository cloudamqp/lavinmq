require "./spec_helper"
require "../src/lavinmq/auth/oauth_user"
require "../src/lavinmq/auth/token_verifier"

class MockJWKSFetcher < JWT::JWKSFetcher
  def initialize
    super("https://auth.example.com", 1.hour)
  end
end

class MockTokenVerifier < LavinMQ::Auth::TokenVerifier
  property should_raise : Exception?
  property return_claims : LavinMQ::Auth::TokenClaims?

  def initialize(config : LavinMQ::Config, fetcher : JWT::JWKSFetcher,
                 @should_raise : Exception? = nil, @return_claims : LavinMQ::Auth::TokenClaims? = nil)
    super(config, fetcher)
  end

  def verify_token(token : String) : LavinMQ::Auth::TokenClaims
    if ex = @should_raise
      raise ex
    end
    @return_claims.not_nil!
  end
end

def create_mock_verifier(should_raise : Exception? = nil, return_claims : LavinMQ::Auth::TokenClaims? = nil)
  config = LavinMQ::Config.new
  config.oauth_issuer_url = "https://auth.example.com"
  config.oauth_preferred_username_claims = ["preferred_username"]
  fetcher = MockJWKSFetcher.new
  MockTokenVerifier.new(config, fetcher, should_raise, return_claims)
end

describe LavinMQ::Auth::OAuthUser do
  describe "#token_lifetime" do
    it "returns positive duration for non-expired token" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      lifetime = user.token_lifetime
      lifetime.should be > 59.minutes
      lifetime.should be <= 1.hour + 1.second
    end

    it "returns negative duration for expired token" do
      expires_at = Time.utc - 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      lifetime = user.token_lifetime
      lifetime.should be < 0.seconds
    end

    it "returns duration close to zero for token expiring soon" do
      expires_at = Time.utc + 5.seconds
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      lifetime = user.token_lifetime
      lifetime.should be > 0.seconds
      lifetime.should be <= 6.seconds
    end
  end

  describe "#on_expiration" do
    it "calls the block when token expires" do
      expires_at = Time.utc + 50.milliseconds
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      callback_called = Channel(Nil).new

      user.on_expiration do
        callback_called.send nil
      end

      select
      when callback_called.receive
        # Expected behavior - callback was called
      when timeout(500.milliseconds)
        fail "Expected expiration callback to be called"
      end
    end

    it "does not call block before expiration" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      callback_called = false

      user.on_expiration do
        callback_called = true
      end

      sleep 50.milliseconds
      callback_called.should be_false
    end
  end

  describe "#cleanup" do
    it "closes the token_updated channel" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      user.cleanup

      # Channel should be closed, attempting to send should raise
      expect_raises(Channel::ClosedError) do
        user.@token_updated.send nil
      end
    end

    it "prevents further operations on closed channel" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      user.cleanup

      # After cleanup, the channel is closed
      # No further operations should work on it
      user.@token_updated.closed?.should be_true
    end
  end

  describe "#update_secret" do
    it "updates user tags from new token" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      user.tags.should be_empty

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      new_claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [LavinMQ::Tag::Administrator] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc + 2.hours
      )
      verifier.return_claims = new_claims

      user.update_secret("new.jwt.token")

      user.tags.should eq [LavinMQ::Tag::Administrator]
      user.cleanup
    end

    it "updates user permissions from new token" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      user.permissions.should be_empty

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      new_permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      new_claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [] of LavinMQ::Tag,
        new_permissions,
        Time.utc + 2.hours
      )
      verifier.return_claims = new_claims

      user.update_secret("new.jwt.token")

      user.permissions.should eq new_permissions
      user.cleanup
    end

    it "updates expiration time from new token" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      old_lifetime = user.token_lifetime

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      new_expires_at = Time.utc + 3.hours
      new_claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        new_expires_at
      )
      verifier.return_claims = new_claims

      user.update_secret("new.jwt.token")

      new_lifetime = user.token_lifetime
      new_lifetime.should be > old_lifetime
      new_lifetime.should be > 2.hours + 59.minutes
      user.cleanup
    end

    it "raises error when username doesn't match" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      wrong_user_claims = LavinMQ::Auth::TokenClaims.new(
        "differentuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc + 2.hours
      )
      verifier.return_claims = wrong_user_claims

      expect_raises(JWT::VerificationError, /Token username mismatch/) do
        user.update_secret("new.jwt.token")
      end

      user.cleanup
    end

    it "updates internal state completely" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      new_claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [LavinMQ::Tag::Monitoring] of LavinMQ::Tag,
        {"/" => {config: /^test/, read: /^test/, write: /^test/}},
        Time.utc + 2.hours
      )
      verifier.return_claims = new_claims

      user.update_secret("new.jwt.token")

      # Verify all fields were updated
      user.token_lifetime.should be > 1.hour + 59.minutes
      user.tags.should eq [LavinMQ::Tag::Monitoring]
      user.permissions.should eq({"/" => {config: /^test/, read: /^test/, write: /^test/}})
      user.cleanup
    end

    it "raises error when token verification fails" do
      expires_at = Time.utc + 1.hour
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at,
        verifier
      )

      # Start expiration fiber to listen on token_updated channel
      user.on_expiration { }

      verifier.should_raise = JWT::DecodeError.new("Invalid token")
      verifier.return_claims = nil

      expect_raises(JWT::DecodeError, /Invalid token/) do
        user.update_secret("invalid.jwt.token")
      end

      user.cleanup
    end
  end

  describe "initialization" do
    it "creates user with given name" do
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "myuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc + 1.hour,
        verifier
      )

      user.name.should eq "myuser"
    end

    it "creates user with given tags" do
      tags = [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring] of LavinMQ::Tag
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "admin",
        tags,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc + 1.hour,
        verifier
      )

      user.tags.should eq tags
    end

    it "creates user with given permissions" do
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      verifier = create_mock_verifier
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        permissions,
        Time.utc + 1.hour,
        verifier
      )

      user.permissions.should eq permissions
    end
  end
end
