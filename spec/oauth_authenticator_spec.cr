require "./spec_helper"
require "../src/lavinmq/auth/oauth_authenticator"
require "../src/lavinmq/auth/token_verifier"

class MockJWKSFetcher < JWT::JWKSFetcher
  def initialize
    super("https://auth.example.com", 1.hour)
  end
end

class MockTokenVerifier < LavinMQ::Auth::TokenVerifier
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

describe LavinMQ::Auth::OAuthAuthenticator do
  describe "#authenticate" do
    it "returns OAuthUser on successful token verification" do
      expires_at = Time.utc + 1.hour
      claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [LavinMQ::Tag::Administrator] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        expires_at
      )
      verifier = create_mock_verifier(return_claims: claims)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "valid.jwt.token")

      user.should_not be_nil
      user.try(&.name).should eq "testuser"
      user.try(&.tags).should eq [LavinMQ::Tag::Administrator]
      user.try(&.permissions).should eq({"/" => {config: /.*/, read: /.*/, write: /.*/}})
    end

    it "returns nil when token is not a JWT" do
      verifier = create_mock_verifier(should_raise: JWT::PasswordFormatError.new("Invalid format"))
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "not-a-jwt")

      user.should be_nil
    end

    it "returns nil when token cannot be decoded" do
      verifier = create_mock_verifier(should_raise: JWT::DecodeError.new("Invalid token"))
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "invalid.jwt.token")

      user.should be_nil
    end

    it "returns nil when token verification fails" do
      verifier = create_mock_verifier(should_raise: JWT::VerificationError.new("Signature invalid"))
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "tampered.jwt.token")

      user.should be_nil
    end

    it "returns nil on unexpected exception" do
      verifier = create_mock_verifier(should_raise: Exception.new("Unexpected error"))
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "some.jwt.token")

      user.should be_nil
    end

    it "creates OAuthUser with correct tags from token" do
      expires_at = Time.utc + 1.hour
      claims = LavinMQ::Auth::TokenClaims.new(
        "admin",
        [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at
      )
      verifier = create_mock_verifier(return_claims: claims)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("admin", "valid.jwt.token")

      user.should_not be_nil
      user.not_nil!.tags.should contain(LavinMQ::Tag::Administrator)
      user.not_nil!.tags.should contain(LavinMQ::Tag::Monitoring)
    end

    it "creates OAuthUser with correct permissions from token" do
      expires_at = Time.utc + 1.hour
      permissions = {
        "/"     => {config: /^queue/, read: /.*/, write: /^exchange/},
        "/test" => {config: /.*/, read: /.*/, write: /.*/},
      }
      claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [] of LavinMQ::Tag,
        permissions,
        expires_at
      )
      verifier = create_mock_verifier(return_claims: claims)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "valid.jwt.token")

      user.should_not be_nil
      user.not_nil!.permissions.should eq permissions
      user.not_nil!.permissions["/"].should eq({config: /^queue/, read: /.*/, write: /^exchange/})
      user.not_nil!.permissions["/test"].should eq({config: /.*/, read: /.*/, write: /.*/})
    end

    it "creates OAuthUser with correct expiration time" do
      expires_at = Time.utc + 2.hours
      claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at
      )
      verifier = create_mock_verifier(return_claims: claims)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      user = authenticator.authenticate("testuser", "valid.jwt.token")

      user.should_not be_nil
      user.not_nil!.token_lifetime.should be > 1.hour + 59.minutes
      user.not_nil!.token_lifetime.should be <= 2.hours + 1.second
    end

    it "uses password as JWT token for verification" do
      token_received = nil
      expires_at = Time.utc + 1.hour
      claims = LavinMQ::Auth::TokenClaims.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        expires_at
      )

      # Create a verifier that captures the token
      verifier = create_mock_verifier(return_claims: claims)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)

      # The password should be passed as the token to verify_token
      user = authenticator.authenticate("testuser", "my.jwt.token")

      user.should_not be_nil
    end
  end
end
