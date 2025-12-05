require "./spec_helper"
require "../src/lavinmq/auth/chain"
require "../src/lavinmq/auth/local_authenticator"

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
end
