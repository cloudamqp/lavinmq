require "./spec_helper"
require "amqp"

describe AvalancheMQ::Server do
  it "rejects invalid password" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(username: "guest", password: "invalid")) do |conn|
      end
    end
  ensure
    s.try &.close
  end

  it "rejects invalid user" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(username: "invalid", password: "guest")) do |conn|
      end
    end
  ensure
    s.try &.close
  end

  it "disallow users who dont have vhost access" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    s.try &.vhosts.create("v1")
    Fiber.yield
    expect_raises(Channel::ClosedError) do
      AMQP::Connection.start(AMQP::Config.new(vhost: "v1", username: "guest", password: "guest")) do |conn|
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
    # s.permissions.create("u1", "v1", /.*/, /.*/, /.*/)
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(vhost: "v1", username: "u1", password: "p1")) do |conn|
      conn.config.vhost.should eq "v1"
      conn.config.username.should eq "u1"
    end
    s.vhosts.delete("v1")
    s.users.delete("u1")
    s.close
  end
end
