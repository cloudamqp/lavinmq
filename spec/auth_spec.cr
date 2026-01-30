require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/authenticators/local"

class MockVerifier < LavinMQ::Auth::JWT::TokenVerifier
  def initialize(config : LavinMQ::Config, @username : String, @tags : Array(LavinMQ::Tag)?,
                 @permissions : Hash(String, LavinMQ::Auth::BaseUser::Permissions)?, @expires_at : Time)
    jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(config.oauth_issuer_url, config.oauth_jwks_cache_ttl)
    super(config, jwks_fetcher)
  end
end

class MockJWKSFetcher < LavinMQ::Auth::JWT::JWKSFetcher
  def initialize
    super(URI.new, 1.seconds)
  end

  def fetch_and_update
  end
end

class SimpleMockVerifier < LavinMQ::Auth::JWT::TokenVerifier
  def initialize(config : LavinMQ::Config = LavinMQ::Config.new)
    config.oauth_issuer_url = URI.new if config.oauth_issuer_url.nil?
    super(config, MockJWKSFetcher.new)
  end
end

describe LavinMQ::Auth::Chain do
  it "creates a default authentication chain if not configured" do
    with_amqp_server do |s|
      verifier = SimpleMockVerifier.new
      chain = LavinMQ::Auth::Chain.create(s.@config.auth_backends, s.@users, verifier)
      chain.@backends.should be_a Array(LavinMQ::Auth::Authenticator)
      chain.@backends.size.should eq 1
    end
  end

  it "successfully authenticates and returns a local user" do
    with_amqp_server do |s|
      verifier = SimpleMockVerifier.new(s.@config)
      chain = LavinMQ::Auth::Chain.create(s.@config.auth_backends, s.@users, verifier)
      ctx = LavinMQ::Auth::Context.new("guest", "guest".to_slice, loopback: true)
      user = chain.authenticate(ctx)
      user.should_not be_nil
    end
  end

  it "does not authenticate when given invalid credentials" do
    with_amqp_server do |s|
      s.@users.create("foo", "bar")
      verifier = SimpleMockVerifier.new(s.@config)
      chain = LavinMQ::Auth::Chain.create(s.@config.auth_backends, s.@users, verifier)
      ctx = LavinMQ::Auth::Context.new("bar", "baz".to_slice, loopback: true)
      user = chain.authenticate(ctx)
      user.should be_nil
    end
  end

  it "requires loopback if Config.#default_user_only_loopback? is true" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.default_user_only_loopback = true
      verifier = SimpleMockVerifier.new(s.@config)
      chain = LavinMQ::Auth::Chain.create(s.@config.auth_backends, s.@users, verifier)
      ctx = LavinMQ::Auth::Context.new("guest", "guest".to_slice, loopback: false)
      user = chain.authenticate(ctx)
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
        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
        chain.@backends.size.should eq 1
        chain.@backends[0].should be_a LavinMQ::Auth::LocalAuthenticator
      end
    end

    it "creates only oauth authenticator when backends is ['oauth']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = URI.parse("https://test-giant-beige-hawk.rmq7.cloudamqp.com/realms/lavinmq-dev/")
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
        chain.@backends.size.should eq 1
        chain.@backends[0].should be_a LavinMQ::Auth::OAuthAuthenticator
      end
    end

    it "creates both authenticators when backends is ['local', 'oauth']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = URI.parse("https://test-giant-beige-hawk.rmq7.cloudamqp.com/realms/lavinmq-dev/")
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
        chain.@backends.size.should eq 2
        chain.@backends[0].should be_a LavinMQ::Auth::LocalAuthenticator
        chain.@backends[1].should be_a LavinMQ::Auth::OAuthAuthenticator
      end
    end

    it "creates authenticators in specified order ['oauth', 'local']" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth", "local"]
      config.oauth_issuer_url = URI.parse("https://test-giant-beige-hawk.rmq7.cloudamqp.com/realms/lavinmq-dev/")
      config.oauth_preferred_username_claims = ["preferred_username"]
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
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
          verifier = SimpleMockVerifier.new(config)
          LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
        end
      end
    end

    it "raises error when invalid backend is mixed with valid ones" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "invalid", "oauth"]
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        expect_raises(Exception, /Unsupported authentication backend: invalid/) do
          verifier = SimpleMockVerifier.new(config)
          LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)
        end
      end
    end
  end

  describe "auth chaining fallback behavior" do
    it "tries local first, then oauth when both configured" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        users.create("testuser", "localpass", [LavinMQ::Tag::Administrator])

        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)

        # Should authenticate with local backend
        ctx = LavinMQ::Auth::Context.new("testuser", "localpass".to_slice, loopback: true)
        user = chain.authenticate ctx
        user.should_not be_nil
        user.should be_a LavinMQ::Auth::User
        user.try(&.name).should eq "testuser"
      end
    end

    it "returns nil when all backends in chain fail" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local", "oauth"]
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]

      with_datadir do |data_dir|
        config.data_dir = data_dir
        users = LavinMQ::Auth::UserStore.new(data_dir, nil)
        users.create("testuser", "correctpass", [LavinMQ::Tag::Administrator])

        verifier = SimpleMockVerifier.new(config)
        chain = LavinMQ::Auth::Chain.create(config.auth_backends, users, verifier)

        # Wrong password for local user, not a JWT for oauth
        ctx = LavinMQ::Auth::Context.new("testuser", "wrongpass".to_slice, loopback: true)
        user = chain.authenticate ctx
        user.should be_nil
      end
    end
  end

  describe "OauthUser" do
    it "token_lifetime returns positive duration for non-expired token" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      verifier = MockVerifier.new(config, "testuser", nil, nil, Time.utc + 1.hour)

      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc + 1.hour,
        verifier
      )

      lifetime = user.token_lifetime
      lifetime.should be > 59.minutes
      lifetime.should be <= 1.hour + 1.second
    end

    it "token_lifetime returns negative duration for expired token" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      verifier = MockVerifier.new(config, "testuser", nil, nil, Time.utc + 1.hour)

      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::BaseUser::Permissions,
        Time.utc - 1.hour,
        verifier
      )

      lifetime = user.token_lifetime
      lifetime.should be < 0.seconds
    end

    describe "on_expiration" do
      it "calls the block when token expires" do
        config = LavinMQ::Config.new
        config.oauth_issuer_url = URI.parse("https://auth.example.com")
        config.oauth_preferred_username_claims = ["preferred_username"]
        verifier = MockVerifier.new(config, "testuser", nil, nil, Time.utc + 50.milliseconds)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::BaseUser::Permissions,
          Time.utc + 50.milliseconds,
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

      # Fails at the moment, but the code around the expiration loop needs to be refactor anyway.
      it "resets expiration timer when token_updated receives" do
        exp = Time.utc + 300.milliseconds
        verifier = MockVerifier.new(LavinMQ::Config.new, "testuser", nil, nil, exp)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::BaseUser::Permissions,
          Time.utc + 300.milliseconds,
          verifier
        )

        callback_called = false

        user.on_expiration do
          callback_called = true
        end

        # Send token update before expiration
        sleep 70.milliseconds

        user.@token_updated.send nil

        # Wait past the original expiration time
        sleep 250.milliseconds

        # Callback should not have been called yet since timer was reset
        # but it will be called eventually (negative token_lifetime triggers immediately)
        callback_called.should be_false
      end

      it "does not call block before expiration" do
        config = LavinMQ::Config.new
        config.oauth_issuer_url = URI.parse("https://auth.example.com")
        config.oauth_preferred_username_claims = ["preferred_username"]
        verifier = MockVerifier.new(LavinMQ::Config.new, "testuser", nil, nil, Time.utc + 1.hour)

        user = LavinMQ::Auth::OAuthUser.new(
          "testuser",
          [] of LavinMQ::Tag,
          {} of String => LavinMQ::Auth::BaseUser::Permissions,
          Time.utc + 1.hour,
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
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      verifier = MockVerifier.new(config, "testuser", nil, nil, Time.utc + 1.hour)

      permissions = {"/" => {config: /^queue/, read: /.*/, write: /^exchange/}}
      user = LavinMQ::Auth::OAuthUser.new(
        "oauthuser",
        [] of LavinMQ::Tag,
        permissions,
        Time.utc + 1.hour,
        verifier
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
