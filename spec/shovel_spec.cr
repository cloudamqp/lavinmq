require "./spec_helper"
require "../src/avalanchemq/shovel"

def setup_qs(conn) : {AMQP::Exchange, AMQP::Queue, AMQP::Queue}
  ch = conn.channel
  x = ch.exchange("", "direct", passive: true)
  q1 = ch.queue("q1")
  q2 = ch.queue("q2")
  {x, q1, q2}
end

def publish(x, rk, msg)
  pmsg = AMQP::Message.new(msg)
  x.publish pmsg, rk
end

describe AvalancheMQ::Shovel do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)

  it "should shovel and stop when queue length is met" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "shovel me"
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

  it "should shovel large messages" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "a" * 10_000

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

  it "should shovel forever" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
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
      publish x, "q1", "shovel me"
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

  it "should shovel with ack mode on-publish" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "shovel me"
      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
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

  it "should shovel with ack mode no-ack" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      publish x, "q1", "shovel me"
      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost,
        ack_mode: AvalancheMQ::Shovel::AckMode::NoAck)
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
    end
  ensure
    close(s)
  end

  it "should shovel with past prefetch" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      x, q1, q2 = setup_qs conn
      100.times do
        publish x, "q1", "shovel me"
      end
      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,
        prefetch: 1_u16
      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
      shovel.run
      wait_for { shovel.stopped? }
      s.not_nil!.vhosts["/"].queues["q1"].message_count.should eq 0
      s.not_nil!.vhosts["/"].queues["q2"].message_count.should eq 100
    end
  ensure
    close(s)
  end

  it "should shovel once qs are declared" do
    s = amqp_server
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      source = AvalancheMQ::Shovel::Source.new(
        "amqp://guest:guest@localhost",
        "q1"
      )
      dest = AvalancheMQ::Shovel::Destination.new(
        "amqp://guest:guest@localhost",
        "q2"
      )
      shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
      shovel.run
      x, q1, q2 = setup_qs conn
      publish x, "q1", "shovel me"
      rmsg = nil
      until rmsg = q2.get(no_ack: true)
        Fiber.yield
      end
      rmsg.to_s.should eq "shovel me"
      shovel.stop
    end
  ensure
    close(s)
  end
end
