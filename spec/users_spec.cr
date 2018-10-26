require "./spec_helper"

describe AvalancheMQ::Server do
  it "rejects invalid password" do
    expect_raises(Channel::ClosedError) do
      with_channel(username: "guest", password: "invalid") { }
    end
  end

  it "rejects invalid user" do
    expect_raises(Channel::ClosedError) do
      with_channel(username: "invalid", password: "guest") { }
    end
  end

  it "disallow users who dont have vhost access" do
    s.vhosts.create("v1")
    s.users.rm_permission("guest", "v1")
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      with_channel(vhost: "v1", username: "guest", password: "guest") { }
    end
  ensure
    s.vhosts.delete("v1")
  end

  it "allows users with access to vhost" do
    s.vhosts.create("v1")
    s.users.create("u1", "p1")
    s.users.add_permission("u1", "v1", /.*/, /.*/, /.*/)
    config = AMQP::Config.new(vhost: "v1", username: "u1", password: "p1", port: AMQP_PORT)
    AMQP::Connection.start(config) do |conn|
      conn.config.vhost.should eq "v1"
      conn.config.username.should eq "u1"
    end
  ensure
    s.vhosts.delete("v1")
    s.users.delete("u1")
  end

  it "prohibits declaring exchanges if don't have access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        ch.exchange("x1", "direct")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "prohibits declaring queues if don't have access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        ch.queue("q1")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "prohibits publish if user doesn't have access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x = ch.exchange("", "direct")
        q = ch.queue("")
        x.publish AMQP::Message.new("msg"), q.name
        q.get
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "prohibits consuming if user doesn't have access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        q = ch.queue("")
        q.subscribe(no_ack: true) { }
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "prohibits getting from queue if user doesn't have access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        q = ch.queue("")
        q.get(no_ack: true)
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "prohibits purging queue if user doesn't have write access" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        q = ch.queue("")
        q.purge
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "allows declaring exchanges passivly even without config perms" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    with_channel(vhost: "v1") do |ch|
      x = ch.exchange("amq.topic", "topic", passive: true)
      x.is_a?(AMQP::Exchange).should be_true
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "allows declaring queues passivly even without config perms" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    with_channel(vhost: "v1") do |ch|
      ch.queue("q1cp", durable: false, auto_delete: true)
    end
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    with_channel(vhost: "v1") do |ch|
      q1 = ch.queue("q1cp", passive: true)
      q1.is_a?(AMQP::Queue).should be_true
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "disallows deleting queues without config perms" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    with_channel(vhost: "v1") do |ch|
      ch.queue("q1", durable: false, auto_delete: true)
    end
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        q1 = ch.queue("q1", passive: true)
        q1.delete
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "disallows deleting exchanges without config perms" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    with_channel(vhost: "v1") do |ch|
      ch.exchange("x1", "direct", durable: false)
    end
    s.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", passive: true)
        x1.delete
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "binding queue required write perm on queue" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    with_channel(vhost: "v1") do |ch|
      ch.exchange("x1", "direct", durable: false)
      ch.queue("q1", durable: false)
    end
    s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.bind(x1, "")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "binding queue required read perm on exchange" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    with_channel(vhost: "v1") do |ch|
      ch.exchange("x1", "direct", durable: false)
      ch.queue("q1", durable: false)
    end
    s.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.bind(x1, "")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "unbinding queue required write perm on queue" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    with_channel(vhost: "v1") do |ch|
      x1 = ch.exchange("x1", "direct", durable: false)
      q1 = ch.queue("q1", durable: false)
      q1.bind(x1, "")
    end
    s.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.unbind(x1, "")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end

  it "unbinding queue required read perm on exchange" do
    s.vhosts.create("v1")
    s.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
    with_channel(vhost: "v1") do |ch|
      x1 = ch.exchange("x1", "direct", durable: false)
      q1 = ch.queue("q1", durable: false)
      q1.bind(x1, "")
    end
    s.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      with_channel(vhost: "v1") do |ch|
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.unbind(x1, "")
      end
    end
  ensure
    s.users.rm_permission("guest", "v1")
    s.vhosts.delete("v1")
  end
end
