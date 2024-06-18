require "./spec_helper"

describe LavinMQ::User do
  describe "#update_password" do
    it "should not update password hash when given same password as current" do
      u = LavinMQ::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      password_hash_before = u.password
      u.update_password("password")
      u.password.should eq password_hash_before
    end

    it "should update password hash when given other password than current" do
      u = LavinMQ::User.create("username", "password", "sha256", [] of LavinMQ::Tag)
      password_hash_before = u.password
      u.update_password("other")
      u.password.should_not eq password_hash_before
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
