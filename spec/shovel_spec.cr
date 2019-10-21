require "./spec_helper"
require "../src/avalanchemq/shovel"

module ShovelSpecHelpers
  def self.setup_qs(ch, prefix = "") : {AMQP::Client::Exchange, AMQP::Client::Queue}
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
    x.publish msg, rk
  end
end

describe AvalancheMQ::Shovel do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL
  vhost = AvalancheMQ::VHost.new("x", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))

  it "should shovel and stop when queue length is met" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "ql_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "ql_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "ql_shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "ql_"
      ShovelSpecHelpers.publish x, "ql_q1", "shovel me"
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).not_nil!.body_io.to_s.should eq "shovel me"
      s.vhosts["/"].shovels.not_nil!.empty?.should be_true
    end
  ensure
    ShovelSpecHelpers.cleanup "ql_"
    shovel.try &.delete
  end

  it "should shovel large messages" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "lm_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "lm_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "lm_shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "lm_"
      ShovelSpecHelpers.publish x, "lm_q1", "a" * 10_000
      shovel.run
      wait_for { shovel.stopped? }
      q2.get(no_ack: true).not_nil!.body_io.to_s.bytesize.should eq 10_000
    end
  ensure
    ShovelSpecHelpers.cleanup "lm_"
    shovel.try &.delete
  end

  it "should shovel forever" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "sf_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::Never
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "sf_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "sf_shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "sf_"
      shovel.run
      ShovelSpecHelpers.publish x, "sf_q1", "shovel me"
      rmsg = nil
      until rmsg = q2.get(no_ack: true)
        Fiber.yield
      end
      rmsg.not_nil!.body_io.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup "sf_"
    shovel.try &.delete
  end

  it "should shovel with ack mode on-publish" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "ap_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "ap_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "ap_shovel", vhost,
      ack_mode: AvalancheMQ::Shovel::AckMode::OnPublish)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "ap_"
      ShovelSpecHelpers.publish x, "ap_q1", "shovel me"
      shovel.run
      msgs = Channel(AMQP::Client::Message).new
      spawn do
        sleep 15
        msgs.close
      end
      q2.subscribe { |msg| msgs.send msg }
      msg = msgs.receive?
      msg.should_not be_nil
      msg.not_nil!.body_io.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup "ap_"
    shovel.try &.delete
  end

  it "should shovel with ack mode no-ack" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "na_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "na_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "na_shovel", vhost,
      ack_mode: AvalancheMQ::Shovel::AckMode::NoAck)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "na_"
      ShovelSpecHelpers.publish x, "na_q1", "shovel me"
      shovel.run
      msgs = Channel(AMQP::Client::Message).new
      spawn do
        sleep 15
        msgs.close
      end
      q2.subscribe { |msg| msgs.send msg }
      msg = msgs.receive?
      msg.should_not be_nil
      msg.not_nil!.body_io.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup "na_"
    shovel.try &.delete
  end

  it "should shovel past prefetch" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "prefetch_q1",
      delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,
      prefetch: 20_u16
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "prefetch_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "prefetch_shovel", vhost)
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
    shovel.try &.delete
  end

  it "should shovel once qs are declared" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}",
      "od_q1"
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}",
      "od_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "od_shovel", vhost)
    with_channel do |ch|
      shovel.run
      x, q2 = ShovelSpecHelpers.setup_qs ch, "od_"
      ShovelSpecHelpers.publish x, "od_q1", "shovel me"
      rmsg = nil
      wait_for { rmsg = q2.get(no_ack: true) }
      rmsg.not_nil!.body_io.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup "od_"
    shovel.try &.delete
  end

  it "should reconnect and continue" do
    p = AvalancheMQ::Parameter.new("shovel", "rc_shovel",
      JSON::Any.new({
        "src-uri"    => JSON::Any.new("#{AMQP_BASE_URL}"),
        "src-queue"  => JSON::Any.new("rc_q1"),
        "dest-uri"   => JSON::Any.new("#{AMQP_BASE_URL}"),
        "dest-queue" => JSON::Any.new("rc_q2"),
      } of String => JSON::Any))
    s.vhosts["/"].add_parameter(p)
    with_channel do |ch|
      q1 = ch.queue("rc_q1", durable: true)
      ch.queue("rc_q2", durable: true)
      props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
      q1.publish_confirm "shovel me", props: props
    end
    sleep 0.01
    close_servers
    TestHelpers.setup
    sleep 0.01
    with_channel do |ch|
      q1 = ch.queue("rc_q1", durable: true)
      q2 = ch.queue("rc_q2", durable: true)
      props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
      q1.publish "shovel me", props: props
      msgs = Channel(AMQP::Client::Message).new(4)
      spawn do
        sleep 5
        msgs.close
      end
      q2.subscribe { |msg| msgs.send msg }
      2.times { msgs.receive?.should_not be_nil }
      s.vhosts["/"].queues["rc_q1"].message_count.should eq 0
    end
  ensure
    s.vhosts["/"].delete_queue("rc_q1")
    s.vhosts["/"].delete_queue("rc_q2")
    s.vhosts["/"].delete_parameter("shovel", "rc_shovel")
  end

  it "should shovel over amqps" do
    source = AvalancheMQ::Shovel::Source.new(
      "#{AMQP_BASE_URL}?verify=none",
      "ssl_q1"
    )
    dest = AvalancheMQ::Shovel::Destination.new(
      "#{AMQP_BASE_URL}?verify=none",
      "ssl_q2"
    )
    shovel = AvalancheMQ::Shovel.new(source, dest, "ssl_shovel", vhost)
    with_channel do |ch|
      x, q2 = ShovelSpecHelpers.setup_qs ch, "ssl_"
      shovel.run
      ShovelSpecHelpers.publish x, "ssl_q1", "shovel me"
      msgs = [] of AMQP::Client::Message
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs[0]?.not_nil!.body_io.to_s.should eq "shovel me"
    end
  ensure
    ShovelSpecHelpers.cleanup "ssl_"
    shovel.try &.delete
  end
end
