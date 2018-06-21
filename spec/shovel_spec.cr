require "./spec_helper"
require "../src/avalanchemq/shovel"

describe AvalancheMQ::Shovel do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)

  it "can shovel and stop when queue length is met" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
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
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
      s.not_nil!.vhosts["/"].shovels.not_nil!.empty?.should be_true
    end
  ensure
    close(s)
  end

  it "can shovel large messages" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      q1 = ch.queue("q1")
      q2 = ch.queue("q2")
      pmsg = AMQP::Message.new("a" * 10_000)
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
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.bytesize.should eq 10_000
    end
  ensure
    close(s)
  end

  it "can shovel forever" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      q1 = ch.queue("q1")
      q2 = ch.queue("q2")
      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::Never
      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
      spawn { shovel.run }
      Fiber.yield
      pmsg = AMQP::Message.new("shovel me")
      x.publish pmsg, "q1"
      Fiber.yield
      rmsg = nil
      until rmsg = q2.get(no_ack: true)
        Fiber.yield
      end
      rmsg.to_s.should eq "shovel me"
      shovel.stop
      Fiber.yield
    end
  ensure
    close(s)
  end

  it "can shovel with ack mode on-publish" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
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
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,

      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost,
        ack_mode: AvalancheMQ::Shovel::AckMode::OnPublish)
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
    end
  ensure
    close(s)
  end
end
