require "./spec_helper"

describe LavinMQ::Auth::OAuthUser do
  describe "#expired?" do
    it "returns true for expired tokens" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc - 1.hour
      )
      user.expired?.should be_true
    end

    it "returns false for valid tokens" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {} of String => LavinMQ::Auth::User::Permissions,
        Time.utc + 1.hour
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
        Time.utc - 1.hour
      )
      cache = LavinMQ::Auth::PermissionCache.new
      user.can_write?("/", "queue1", cache).should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
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
        Time.utc - 1.hour
      )
      user.can_read?("/", "queue1").should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
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
        Time.utc - 1.hour
      )
      user.can_config?("/", "queue1").should be_false
    end

    it "allows access when token is valid and permissions match" do
      user = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [] of LavinMQ::Tag,
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user.can_config?("/", "queue1").should be_true
    end
  end

  describe "#same_identity?" do
    it "returns true for identical users" do
      user1 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user2 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 2.hours
      )
      user1.same_identity?(user2).should be_true
    end

    it "returns false for different usernames" do
      user1 = LavinMQ::Auth::OAuthUser.new(
        "user1",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user2 = LavinMQ::Auth::OAuthUser.new(
        "user2",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user1.same_identity?(user2).should be_false
    end

    it "returns false for different permissions" do
      user1 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user2 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /^$/, read: /^$/, write: /^$/}},
        Time.utc + 1.hour
      )
      user1.same_identity?(user2).should be_false
    end

    it "returns false for different tags" do
      user1 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Administrator],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user2 = LavinMQ::Auth::OAuthUser.new(
        "testuser",
        [LavinMQ::Tag::Monitoring],
        {"/" => {config: /.*/, read: /.*/, write: /.*/}},
        Time.utc + 1.hour
      )
      user1.same_identity?(user2).should be_false
    end
  end
end
