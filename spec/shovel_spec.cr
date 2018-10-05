require "./spec_helper"
require "../src/avalanchemq/shovel"

module ShovelSpecHelpers
  def self.setup_qs(ch, prefix = "") : {AMQP::Exchange, AMQP::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("#{prefix}q1")
    q2 = ch.queue("#{prefix}q2")
    {x, q2}
  end

  def self.cleanup(prefix = "")
    s.vhosts["/"].delete_queue("#{prefix}q1")
    s.vhosts["/"].delete_queue("#{prefix}q2")
  end

  def self.publish(x, rk, msg)
    pmsg = AMQP::Message.new(msg)
    x.publish pmsg, rk
  end
end

describe AvalancheMQ::Shovel do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log)

  it "should shovel and stop when queue length is met" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
      s.vhosts["/"].shovels.not_nil!.empty?.should be_true
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should shovel large messages" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      ShovelSpecHelpers.publish x, "q1", "a" * 10_000
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.bytesize.should eq 10_000
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should shovel forever" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::Never
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      shovel.run
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      rmsg = nil
      until rmsg = q2.get(no_ack: true)
        Fiber.yield
      end
      rmsg.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should shovel with ack mode on-publish" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost,
      ack_mode: AvalancheMQ::Shovel::AckMode::OnPublish)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should shovel with ack mode no-ack" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost,
      ack_mode: AvalancheMQ::Shovel::AckMode::NoAck)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should shovel past prefetch" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "prefetch_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,
      prefetch: 1_u16
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "prefetch_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      x = ShovelSpecHelpers.setup_qs(ch, "prefetch_").first
      100.times do
        ShovelSpecHelpers.publish x, "prefetch_q1", "shovel me"
      end
      wait_for { s.vhosts["/"].queues["prefetch_q1"].message_count == 100 }
      shovel.run
      wait_for { shovel.stopped? }
      s.vhosts["/"].queues["prefetch_q1"].message_count.should eq 0
      s.vhosts["/"].queues["prefetch_q2"].message_count.should eq 100
    end
  ensure
    ShovelSpecHelpers.cleanup("prefetch_")
    shovel.try &.stop
  end

  it "should shovel once qs are declared" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "q1"
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      shovel.run
      x, q2 = ShovelSpecHelpers.setup_qs ch
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      rmsg = nil
      until rmsg = q2.get(no_ack: true)
        Fiber.yield
      end
      rmsg.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end

  it "should reconnect and continue" do
    p = AvalancheMQ::Parameter.new("shovel", "shovel",
      JSON::Any.new({
        "src-uri"    => JSON::Any.new("#{AMQP_BASE_URL}"),
        "src-queue"  => JSON::Any.new("q1d"),
        "dest-uri"   => JSON::Any.new("#{AMQP_BASE_URL}"),
        "dest-queue" => JSON::Any.new("q2d"),
      } of String => JSON::Any))
    s.vhosts["/"].add_parameter(p)
    with_channel do |ch|
      x = ch.exchange("", "direct", passive: true)
      ch.queue("q1d", durable: true)
      ch.queue("q2d", durable: true)
      props = AMQP::Protocol::Properties.new(delivery_mode: 2_u8)
      pmsg = AMQP::Message.new("shovel me", props)
      x.publish pmsg, "q1d"
    end
    close_servers
    TestHelpers.setup

    Fiber.yield
    with_channel do |ch|
      x = ch.exchange("", "direct", passive: true)
      ch.queue("q1d", durable: true)
      q2 = ch.queue("q2d", durable: true)
      ShovelSpecHelpers.publish x, "q1d", "shovel me"
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 2 }
      s.vhosts["/"].queues["q1d"].message_count.should eq 0
      msgs.size.should eq 2
    end
  ensure
    s.vhosts["/"].delete_queue("q1d")
    s.vhosts["/"].delete_queue("q2d")
    s.vhosts["/"].delete_parameter("shovel", "shovel")
  end

  it "should shovel over amqps" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}?verify=none",
      "q1"
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}?verify=none",
      "q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch
      shovel.run
      ShovelSpecHelpers.publish x, "q1", "shovel me"
      msgs = [] of AMQP::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs[0]?.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup
    shovel.try &.stop
  end
end
