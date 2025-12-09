require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/local_authenticator"

class MockAuthenticator
  include LavinMQ::Auth::TokenVerifier

  def initialize(@username : String, @tags : Array(LavinMQ::Tag)?,
                 @permissions : Hash(String, LavinMQ::Auth::User::Permissions)?, @expires_at : Time)
  end

  def verify_token(token : String) : TokenClaims
    tags = @tags || Array(LavinMQ::Tag).new
    permissions = @permissions || Hash(String, LavinMQ::Auth::User::Permissions).new
    TokenClaims.new(@username, tags, permissions, @expires_at)
  end
end

describe LavinMQ::Auth::Chain do
  it "Creates a default authentication chain if not configured" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      chain.@backends.should be_a Array(LavinMQ::Auth::Authenticator)
      chain.@backends.size.should eq 1
    end
  end

  it "Successfully authenticates and returns a local user" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      user = chain.authenticate("guest", "guest")
      user.should_not be_nil
    end
  end

  it "Does not authenticate when given invalid credentials" do
    with_amqp_server do |s|
      chain = LavinMQ::Auth::Chain.create(s.@config, s.@users)
      user = chain.authenticate("guest", "invalid")
      user.should be_nil
    end
  end

  describe "auth backend configuration" do
    it "creates only local authenticator when backends is ['local']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        chain = LavinMQ::Auth::Chain.create(config, users)
        chain.@backends.size.should eq 1
        chain.@backends[0].should be_a LavinMQ::Auth::LocalAuthenticator
      end
    end

    it "creates only oauth authenticator when backends is ['oauth']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        chain = LavinMQ::Auth::Chain.create(config, users)
        chain.@backends.size.should eq 1
        chain.@backends[0].should be_a LavinMQ::Auth::OAuthAuthenticator
      end
    end

    it "creates both authenticators when backends is ['local', 'oauth']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        chain = LavinMQ::Auth::Chain.create(config, users)
        chain.@backends.size.should eq 2
        chain.@backends[0].should be_a LavinMQ::Auth::LocalAuthenticator
        chain.@backends[1].should be_a LavinMQ::Auth::OAuthAuthenticator
      end
    end

    it "creates authenticators in specified order ['oauth', 'local']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth", "local"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        chain = LavinMQ::Auth::Chain.create(config, users)
        chain.@backends.size.should eq 2
        chain.@backends[0].should be_a LavinMQ::Auth::OAuthAuthenticator
        chain.@backends[1].should be_a LavinMQ::Auth::LocalAuthenticator
      end
    end

    it "raises error for unsupported backend" do
      config = LavinMQ::Config.new
      config.auth_backends = ["unsupported"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        expect_raises(Exception, /Unsupported authentication backend/) do
          LavinMQ::Auth::Chain.create(config, users)
        end
      end
    end

    it "raises error when invalid backend is mixed with valid ones" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "invalid", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        expect_raises(Exception, /Unsupported authentication backend: invalid/) do
          LavinMQ::Auth::Chain.create(config, users)
        end
      end
    end
  end

  describe "auth chaining fallback behavior" do
    it "tries local first, then oauth when both configured" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        users.create("testuser", "localpass", [LavinMQ::Tag::Administrator])

        chain = LavinMQ::Auth::Chain.create(config, users)

        # Should authenticate with local backend
        user = chain.authenticate("testuser", "localpass")
        user.should_not be_nil
        user.should be_a LavinMQ::Auth::User
        user.try(&.name).should eq "testuser"
      end
    end

    it "falls back to oauth when local authentication fails" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)

        chain = LavinMQ::Auth::Chain.create(config, users)

        # Local will fail (user doesn't exist), but should fall back to oauth
        # OAuth will also fail because we don't have a valid JWT, but we can
        # verify the chain tries both by checking that oauth is attempted
        # (it will return nil because password is not a JWT)
        user = chain.authenticate("oauthuser", "not-a-jwt")
        user.should be_nil # Both backends failed
      end
    end

    it "returns nil when all backends in chain fail" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        users.create("testuser", "correctpass", [LavinMQ::Tag::Administrator])

        chain = LavinMQ::Auth::Chain.create(config, users)

        # Wrong password for local user, not a JWT for oauth
        user = chain.authenticate("testuser", "wrongpass")
        user.should be_nil
      end
    end

    it "stops at first successful authentication" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        users.create("testuser", "localpass", [LavinMQ::Tag::Management])

        chain = LavinMQ::Auth::Chain.create(config, users)

        # Should stop at local backend and not try oauth
        user = chain.authenticate("testuser", "localpass")
        user.should_not be_nil
        user.should be_a LavinMQ::Auth::User
        user.try(&.name).should eq "testuser"
        # Verify it's the local user (has the Management tag we set)
        if u = user
          u.tags.should contain(LavinMQ::Tag::Management)
        end
      end
    end
  end

  describe "OauthUser" do
    it "token_lifetime returns positive duration for non-expired token" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc + 1.hour,
        authenticator
      )

      lifetime = user.token_lifetime
      lifetime.should be > 59.minutes
      lifetime.should be <= 1.hour + 1.second
    end

    it "token_lifetime returns negative duration for expired token" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc - 1.hour,
        authenticator
      )

      lifetime = user.token_lifetime
      lifetime.should be < 0.seconds
    end

    describe "on_expiration" do
      it "calls the block when token expires" do
        config = LavinMQ::Config.new
        config.oauth_issuer_url = "https://auth.example.com"
        config.oauth_preferred_username_claims = ["preferred_username"]
        authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::User::Permissions,
          Time.utc + 50.milliseconds,
          authenticator
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

      it "resets expiration timer when token_updated receives" do
        exp = Time.utc + 300.milliseconds
        authenticator = MockAuthenticator.new("testuser", nil, nil, exp)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::User::Permissions,
          Time.utc + 100.milliseconds,
          authenticator
        )

        callback_called = false

        user.on_expiration do
          callback_called = true
        end

        # Send token update before expiration
        sleep 70.milliseconds

        user.update_secret("")
        # user.token_updated.send nil

        # Wait past the original expiration time
        sleep 70.milliseconds

        # Callback should not have been called yet since timer was reset
        # but it will be called eventually (negative token_lifetime triggers immediately)
        callback_called.should be_false
      end

      it "does not call block before expiration" do
        config = LavinMQ::Config.new
        config.oauth_issuer_url = "https://auth.example.com"
        config.oauth_preferred_username_claims = ["preferred_username"]
        authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::User::Permissions,
          Time.utc + 1.hour,
          authenticator
        )

        callback_called = false

        user.on_expiration do
          callback_called = true
        end

        sleep 50.milliseconds
        callback_called.should be_false
      end
    end
  end

  describe "permissions_details" do
    it "returns correct permission details for local user" do
      with_datadir do |data_dir|
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        user = users.create("testuser", "password", [LavinMQ::Tag::Administrator])
        user.permissions["/"] = {config: /.*/, read: /.*/, write: /.*/}

        details = user.permissions_details("/", user.permissions["/"])

        details[:user].should eq "testuser"
        details[:vhost].should eq "/"
        details[:configure].should eq /.*/
        details[:read].should eq /.*/
        details[:write].should eq /.*/
      end
    end

    it "returns correct permission details for OAuth user" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username"]
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(config)

      permissions = {"/" => {config: /^queue/, read: /.*/, write: /^exchange/}}
      user = LavinMQ::Auth::OAuthUser.new(
        "oauthuser",
        [] of LavinMQ::Tag,
        permissions,
        Time.utc + 1.hour,
        authenticator
      )

      details = user.permissions_details("/", permissions["/"])

      details[:user].should eq "oauthuser"
      details[:vhost].should eq "/"
      details[:configure].should eq /^queue/
      details[:read].should eq /.*/
      details[:write].should eq /^exchange/
    end
  end
end
