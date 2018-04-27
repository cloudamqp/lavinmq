require "./spec_helper"

describe AvalancheMQ::Server do
  it "rejects invalid password" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(username: "guest",
                                              password: "invalid")) { }
    end
  ensure
    s.try &.close
  end

  it "rejects invalid user" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(username: "invalid",
                                              password: "guest")) do |conn|
      end
    end
  ensure
    s.try &.close
  end

  it "disallow users who dont have vhost access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.rm_permission("guest", "v1")
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1", username: "guest",
                                              password: "guest")) do |conn|
      end
    end
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "allows users with access to vhost" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    s.vhosts.create("v1")
    s.users.create("u1", "p1")
    s.users.add_permission("u1", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1", username: "u1", password: "p1")) do |conn|
      conn.config.vhost.should eq "v1"
      conn.config.username.should eq "u1"
    end
    s.vhosts.delete("v1")
    s.users.delete("u1")
    s.close
  end

  it "prohibits declaring exchanges if don't have access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x = ch.exchange("x1", "direct")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "prohibits declaring queues if don't have access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x = ch.queue("q1")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "prohibits publish if user doesn't have access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /.*/, /^$/)
    # s.permissions.create("u1", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x = ch.exchange("", "direct")
        q = ch.queue("")
        x.publish AMQP::Message.new("msg"), q.name
        q.get
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "prohibits consuming if user doesn't have access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        q = ch.queue("")
        q.subscribe(no_ack: true) { }
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "prohibits getting from queue if user doesn't have access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /.*/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        q = ch.queue("")
        q.get(no_ack: true)
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "prohibits purging queue if user doesn't have write access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    Fiber.yield
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        q = ch.queue("")
        q.purge
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "allows declaring exchanges passivly even without config perms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      x = ch.exchange("amq.topic", "topic", passive: true)
      x.is_a?(AMQP::Exchange).should be_true
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "allows declaring queues passivly even without config perms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      q1 = ch.queue("q1", durable: false, auto_delete: true)
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      q1 = ch.queue("q1", passive: true)
      q1.is_a?(AMQP::Queue).should be_true
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "disallows deleting queues without config perms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      q1 = ch.queue("q1", durable: false, auto_delete: true)
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        q1 = ch.queue("q1", passive: true)
        q1.delete
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "disallows deleting exchanges without config perms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      ch.exchange("x1", "direct", durable: false)
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x1 = ch.exchange("x1", "direct", passive: true)
        x1.delete
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "binding queue required write perm on queue" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      ch.exchange("x1", "direct", durable: false)
      ch.queue("q1", durable: false)
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.bind(x1, "")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "binding queue required read perm on exchange" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /^$/, /^$/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      ch.exchange("x1", "direct", durable: false)
      ch.queue("q1", durable: false)
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.bind(x1, "")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "unbinding queue required write perm on queue" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      x1 = ch.exchange("x1", "direct", durable: false)
      q1 = ch.queue("q1", durable: false)
      q1.bind(x1, "")
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /.*/, /^$/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.unbind(x1, "")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end

  it "unbinding queue required read perm on exchange" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    s.try &.users.add_permission("guest", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
      ch = conn.channel
      x1 = ch.exchange("x1", "direct", durable: false)
      q1 = ch.queue("q1", durable: false)
      q1.bind(x1, "")
    end
    s.try &.users.add_permission("guest", "v1", /^$/, /^$/, /.*/)
    expect_raises(AMQP::ChannelClosed, /403/) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1")) do |conn|
        ch = conn.channel
        x1 = ch.exchange("x1", "direct", passive: true)
        q1 = ch.queue("q1", passive: true)
        q1.unbind(x1, "")
      end
    end
    s.try &.users.rm_permission("guest", "v1")
    s.try &.vhosts.delete("v1")
  ensure
    s.try &.close
  end
end
