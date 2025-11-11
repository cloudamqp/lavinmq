require "./spec_helper"

describe LavinMQ::Auth::User do
  describe "#update_password" do
    it "should not update password hash when given same password as current" do
      u = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      password_hash_before = u.password
      u.update_password("password")
      u.password.should eq password_hash_before
    end

    it "should update password hash when given other password than current" do
      u = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      password_hash_before = u.password
      u.update_password("other")
      u.password.should_not eq password_hash_before
    end
  end

  describe "Permission cache" do
    it "should have initial revision of 0" do
      cache = LavinMQ::Auth::PermissionCache.new
      cache.revision.should eq 0
    end

    it "should actually use cached results (returns stale data until revision changes)" do
      user = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      user.permissions["vhost1"] = {config: /.*/, read: /.*/, write: /^queue.*/}
      cache = LavinMQ::Auth::PermissionCache.new

      # Cache the result - queue1 should be allowed
      user.can_write?("vhost1", "queue1", cache).should be_true

      # Change permissions directly WITHOUT clearing cache
      # Now only exchanges should be allowed, not queues
      user.permissions["vhost1"] = {config: /.*/, read: /.*/, write: /^exchange.*/}

      # Should return OLD cached result (stale data), proving cache is actually used
      user.can_write?("vhost1", "queue1", cache).should be_true

      # Now clear the cache - should reflect new permissions
      user.clear_permissions_cache

      # Should now return false based on new permissions
      user.can_write?("vhost1", "queue1", cache).should be_false
      # And exchanges should now be allowed
      user.can_write?("vhost1", "exchange1", cache).should be_true
    end

    it "should clear cache when revision changes" do
      user = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      user.permissions["vhost1"] = {config: /.*/, read: /.*/, write: /^queue.*/}
      cache = LavinMQ::Auth::PermissionCache.new

      # First call populates cache
      user.can_write?("vhost1", "queue1", cache).should be_true
      cache.size.should eq 1

      # Clear permissions cache - increments revision
      user.clear_permissions_cache

      # Next call should detect revision mismatch, clear cache, and recalculate
      user.can_write?("vhost1", "queue1", cache).should be_true
      cache.size.should eq 1
      cache.revision.should eq 1
    end

    it "should handle multiple cache clears" do
      user = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      user.permissions["vhost1"] = {config: /.*/, read: /.*/, write: /^queue.*/}
      cache = LavinMQ::Auth::PermissionCache.new

      user.can_write?("vhost1", "queue1", cache)

      # Multiple clears
      user.clear_permissions_cache
      user.clear_permissions_cache
      user.clear_permissions_cache

      # Revision should be updated
      user.can_write?("vhost1", "queue1", cache)
      cache.revision.should eq 3
    end

    it "should handle multiple vhosts in the same cache" do
      user = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      user.permissions["vhost1"] = {config: /.*/, read: /.*/, write: /^queue.*/}
      user.permissions["vhost2"] = {config: /.*/, read: /.*/, write: /^exchange.*/}
      cache = LavinMQ::Auth::PermissionCache.new

      # Same resource name but different vhosts should have different results
      user.can_write?("vhost1", "queue1", cache).should be_true
      user.can_write?("vhost2", "queue1", cache).should be_false

      # Different resources in different vhosts
      user.can_write?("vhost2", "exchange1", cache).should be_true
      user.can_write?("vhost1", "exchange1", cache).should be_false

      # All four checks should be cached independently
      cache.size.should eq 4
    end

    it "should handle vhost with no permissions" do
      user = LavinMQ::Auth::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      cache = LavinMQ::Auth::PermissionCache.new

      # Check permission for vhost with no permissions defined
      user.can_write?("nonexistent", "queue1", cache).should be_false

      # Change to allow everything and verify cache still returns false (stale)
      user.permissions["nonexistent"] = {config: /.*/, read: /.*/, write: /.*/}
      user.can_write?("nonexistent", "queue1", cache).should be_false

      # After clearing, should reflect new permissions
      user.clear_permissions_cache
      user.can_write?("nonexistent", "queue1", cache).should be_true
    end
  end
end

describe LavinMQ::Server do
  it "rejects invalid password" do
    with_amqp_server do |s|
      expect_raises(AMQP::Client::Connection::ClosedException) do
        with_channel(s, user: "guest", password: "invalid") { }
      end
    end
  end

  it "rejects invalid user" do
    with_amqp_server do |s|
      expect_raises(AMQP::Client::Connection::ClosedException) do
        with_channel(s, user: "invalid", password: "guest") { }
      end
    end
  end

  it "disallow users who dont have vhost access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.rm_permission("guest", "v1")
      Fiber.yield
      expect_raises(AMQP::Client::Connection::ClosedException) do
        with_channel(s, vhost: "v1", user: "guest", password: "guest") { }
      end
    end
  end

  it "allows users with access to vhost" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.create("u1", "p1")
      s.users.add_permission("u1", "v1", /.*/, /.*/, /.*/)
      with_channel(s, vhost: "v1", user: "u1", password: "p1") { }
    end
  end

  it "prohibits declaring exchanges if don't have access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          ch.exchange("x1", "direct")
        end
      end
    end
  end

  it "prohibits declaring queues if don't have access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      Fiber.yield
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          ch.queue("q1")
        end
      end
    end
  end

  it "prohibits publish if user doesn't have access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /.*/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          q = ch.queue("")
          q.publish_confirm "msg"
          q.get
        end
      end
    end
  end

  it "prohibits consuming if user doesn't have access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          q = ch.queue("")
          q.subscribe(no_ack: true) { }
        end
      end
    end
  end

  it "prohibits getting from queue if user doesn't have access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          q = ch.queue("")
          q.get(no_ack: true)
        end
      end
    end
  end

  it "prohibits purging queue if user doesn't have write access" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          q = ch.queue("")
          q.purge
        end
      end
    end
  end

  it "allows declaring exchanges passivly even without config perms" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      with_channel(s, vhost: "v1") do |ch|
        x = ch.exchange("amq.topic", "topic", passive: true)
        x.is_a?(AMQP::Client::Exchange).should be_true
      end
    end
  end

  it "allows declaring queues passivly even without config perms" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
      with_channel(s, vhost: "v1") do |ch|
        ch.queue("q1cp", durable: false, auto_delete: true)
      end
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      with_channel(s, vhost: "v1") do |ch|
        q1 = ch.queue("q1cp", passive: true)
        q1.is_a?(AMQP::Client::Queue).should be_true
      end
    end
  end

  it "disallows deleting queues without config perms" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
      Fiber.yield
      with_channel(s, vhost: "v1") do |ch|
        ch.queue("q1", durable: false, auto_delete: true)
      end
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          q1 = ch.queue("q1", passive: true)
          q1.delete
        end
      end
    end
  end

  it "disallows deleting exchanges without config perms" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
      with_channel(s, vhost: "v1") do |ch|
        ch.exchange("x1", "direct", durable: false)
      end
      s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          x1 = ch.exchange("x1", "direct", passive: true)
          x1.delete
        end
      end
    end
  end

  it "binding queue required write perm on queue" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
      Fiber.yield
      with_channel(s, vhost: "v1") do |ch|
        ch.exchange("x1", "direct", durable: false)
        ch.queue("q1", durable: false)
      end
      s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          ch.exchange("x1", "direct", passive: true)
          q1 = ch.queue("q1", passive: true)
          q1.bind("x1", "")
        end
      end
    end
  end

  it "binding queue required read perm on exchange" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
      with_channel(s, vhost: "v1") do |ch|
        ch.exchange("x1", "direct", durable: false)
        ch.queue("q1", durable: false)
      end
      s.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          x1 = ch.exchange("x1", "direct", passive: true)
          q1 = ch.queue("q1", passive: true)
          q1.bind(x1.name, "")
        end
      end
    end
  end

  it "unbinding queue required write perm on queue" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
      Fiber.yield
      with_channel(s, vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", durable: false)
        q1 = ch.queue("q1", durable: false)
        q1.bind(x1.name, "")
      end
      s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          x1 = ch.exchange("x1", "direct", passive: true)
          q1 = ch.queue("q1", passive: true)
          q1.unbind(x1.name, "")
        end
      end
    end
  end

  it "unbinding queue required read perm on exchange" do
    with_amqp_server do |s|
      s.vhosts.create("v1")
      s.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
      with_channel(s, vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", durable: false)
        q1 = ch.queue("q1", durable: false)
        q1.bind(x1.name, "")
      end
      s.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
      expect_raises(AMQP::Client::Channel::ClosedException, /403/) do
        with_channel(s, vhost: "v1") do |ch|
          x1 = ch.exchange("x1", "direct", passive: true)
          q1 = ch.queue("q1", passive: true)
          q1.unbind(x1.name, "")
        end
      end
    end
  end

  it "allows changing default user" do
    LavinMQ::Config.instance.default_user = "spec"
    LavinMQ::Config.instance.default_password = LavinMQ::Auth::User.hash_password("spec", "SHA256").to_s
    with_amqp_server do |s|
      with_channel(s, user: "spec", password: "spec") { }
    end
  ensure
    LavinMQ::Config.instance.default_user = "guest"
    LavinMQ::Config.instance.default_password = LavinMQ::Auth::User.hash_password("guest", "SHA256").to_s
  end

  it "disallows 'guest' if default user is changed" do
    LavinMQ::Config.instance.default_user = "spec"
    LavinMQ::Config.instance.default_password = LavinMQ::Auth::User.hash_password("spec", "SHA256").to_s
    with_amqp_server do |s|
      expect_raises(AMQP::Client::Connection::ClosedException) do
        with_channel(s, user: "guest", password: "guest") { }
      end
    end
  ensure
    LavinMQ::Config.instance.default_user = "guest"
    LavinMQ::Config.instance.default_password = LavinMQ::Auth::User.hash_password("guest", "SHA256").to_s
  end
end

describe LavinMQ::Tag do
  it "parse comma separated list" do
    # Management
    # PolicyMaker
    LavinMQ::Tag
      .parse_list("administrator")
      .should eq [LavinMQ::Tag::Administrator]

    LavinMQ::Tag
      .parse_list("administrator,monitoring")
      .should eq [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring]

    LavinMQ::Tag
      .parse_list("administrator, monitoring")
      .should eq [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring]

    LavinMQ::Tag
      .parse_list("administrator, other")
      .should eq [LavinMQ::Tag::Administrator]

    LavinMQ::Tag
      .parse_list("policymaker")
      .should eq [LavinMQ::Tag::PolicyMaker]

    LavinMQ::Tag
      .parse_list("administrator, monitoring, Management, PolicyMaker")
      .should eq [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring,
                  LavinMQ::Tag::Management, LavinMQ::Tag::PolicyMaker]
  end
end
