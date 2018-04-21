require "./spec_helper"
require "../src/avalanchemq/shovel"
require "amqp"

describe AvalancheMQ::Shovel do
  it "can connect and disconnect" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      q1 = ch.queue("q1")
      q2 = ch.queue("q2")
      pmsg = AMQP::Message.new("shovel me")
      x.publish pmsg, "q1"

      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest)
      shovel.run
      q2.get.to_s.should eq "shovel me"
    end
    s.close
  end
end
