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
end
