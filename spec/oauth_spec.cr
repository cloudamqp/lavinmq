require "./spec_helper"

describe LavinMQ::Auth::OAuthUser do
  mock_auth = LavinMQ::Auth::OAuthAuthenticator.new

  describe "#expired?" do
    it "returns true for expired tokens" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc - 1.hour,
        mock_auth
      )
      user.expired?.should be_true
    end

    it "returns false for valid tokens" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc + 1.hour,
        mock_auth
      )
      user.expired?.should be_false
    end
  end

  describe "#can_write?" do
    it "denies access when token is expired" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc - 1.hour,
        mock_auth
      )
      cache = LavinMQ::Auth::PermissionCache.new
      user.can_write?("/", "queue1", cache).should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour,
        mock_auth
      )
      cache = LavinMQ::Auth::PermissionCache.new
      user.can_write?("/", "queue1", cache).should be_true
    end
  end

  describe "#can_read?" do
    it "denies access when token is expired" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc - 1.hour,
        mock_auth
      )
      user.can_read?("/", "queue1").should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour,
        mock_auth
      )
      user.can_read?("/", "queue1").should be_true
    end
  end

  describe "#can_config?" do
    it "denies access when token is expired" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc - 1.hour,
        mock_auth
      )
      user.can_config?("/", "queue1").should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour,
        mock_auth
      )
      user.can_config?("/", "queue1").should be_true
    end
  end
end
