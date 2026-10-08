require "./spec_helper"

class LavinMQ::AMQP::Channel
  def spec_next_delivery_tag(queue, sp, consumer)
    next_delivery_tag(queue, sp, false, consumer)
  end
end

# Records delivery tags in the order the client reads them off the socket
# (consumer callbacks run in one fiber per consumer, so they can reorder)
class AMQP::Client::Channel
  property wire_tags : ::Channel(UInt64)? = nil

  def incoming(frame)
    if frame.is_a?(AMQ::Protocol::Frame::Basic::Deliver)
      @wire_tags.try &.send(frame.delivery_tag)
    end
    previous_def
  end
end

class LavinMQ::AMQP::Consumer
  # Records whether the channel had global prefetch capacity at each recover
  # redelivery, when set
  class_property capacity_on_recover : Array(Bool)? = nil

  def deliver(msg, sp, redelivered = false, recover = false)
    if recover && (recorded = @@capacity_on_recover)
      recorded << @channel.has_capacity?
    end
    previous_def
  end
end

require "amqp-client"

describe LavinMQ::AMQP::Channel do
  it "should respect consumer_max_per_channel config" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 1
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      expect_raises(AMQP::Client::Channel::ClosedException, /RESOURCE_ERROR/) do
        channel.basic_consume("test:queue:2") { }
      end
    end
  end

  it "consumer_max_per_channel = 0 allows unlimited consumers" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 0
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      channel.basic_consume("test:queue:1") { }
    end
  end
end

# The unacked deque is binary searched on ack, so it must be sorted by
# delivery tag even when consumers on different queues deliver in parallel
describe "LavinMQ::AMQP::Channel delivery tags" do
  it "keeps unacked messages ordered with parallel deliveries", tags: "slow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("tag-order")
        q.subscribe(no_ack: false) { }
        server_ch = s.connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        should_eventually(be_true) { server_ch.consumers.size == 1 }
        consumer = server_ch.consumers.first
        queue = s.vhosts["/"].queue("tag-order")
        sp = LavinMQ::SegmentPosition.new(1u32, 4u32, 1u32)

        begin
          ctx = Fiber::ExecutionContext::Parallel.new("tag-order", 4)
          wg = WaitGroup.new
          4.times do
            wg.add(1)
            ctx.spawn do
              25_000.times { server_ch.spec_next_delivery_tag(queue, sp, consumer) }
            ensure
              wg.done
            end
          end
          wg.wait

          tags = server_ch.@unacked.map(&.tag)
          tags.size.should eq 100_000
          tags.each_cons_pair.count { |a, b| a > b }.should eq 0
        ensure
          # The unacks point to messages that don't exist, so don't let the
          # channel requeue them when it closes
          server_ch.@unack_lock.synchronize { server_ch.@unacked.clear }
        end
      end
    end
  end
end

# A client may ack tag N with multiple=true as soon as it has seen N, so
# tags must reach the socket in the order they are handed out, also when
# consumers on different queues deliver from different threads.
describe "LavinMQ::AMQP::Channel delivery tags on the wire" do
  it "sends delivery tags in increasing order with parallel deliveries", tags: "slow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        vhost = s.vhosts["/"]
        tags = Channel(UInt64).new(100_000)
        ch.wire_tags = tags
        names = {"wire-order-1", "wire-order-2", "wire-order-3", "wire-order-4"}
        names.each do |name|
          q = ch.queue(name)
          q.subscribe(no_ack: false) { }
          1000.times { vhost.publish(LavinMQ::Message.new("", name, "x")) }
        end
        server_ch = s.connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        # The consumers' own deliver loops send these 4000 first
        received = Array(UInt64).new(8000)
        4000.times { received << tags.receive }

        ctx = Fiber::ExecutionContext::Parallel.new("wire-order", 4)
        wg = WaitGroup.new
        names.each do |name|
          q = vhost.queue(name)
          consumer = server_ch.consumers.find!(&.queue.same?(q))
          wg.add(1)
          ctx.spawn do
            1000.times { vhost.publish(LavinMQ::Message.new("", name, "x")) }
            # Deliver from 4 threads at once, bypassing the deliver loops
            while q.basic_get(no_ack: true) { |env| consumer.deliver(env.message, env.segment_position) }
            end
          ensure
            wg.done
          end
        end
        wg.wait
        until received.size >= 8000
          select
          when tag = tags.receive
            received << tag
          when timeout(5.seconds)
            break
          end
        end
        received.each_cons_pair.count { |a, b| a >= b }.should eq 0
      ensure
        # The extra deliveries took their messages with basic_get, so don't
        # let the channel requeue them when it closes
        if server_ch
          server_ch.@unack_lock.synchronize { server_ch.@unacked.clear }
        end
      end
    end
  end
end

describe "LavinMQ::AMQP::Channel basic.recover" do
  # The redelivered messages leave @unacked while they're delivered again, so
  # they must still count against the global prefetch, or the channel's
  # consumers could deliver new messages meanwhile
  it "keeps the global prefetch while redelivering with requeue=false" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.prefetch(1, global: true)
        q = ch.queue
        2.times { q.publish "m" }
        msgs = Channel(AMQP::Client::DeliverMessage).new(10)
        q.subscribe(no_ack: false) { |m| msgs.send m }
        msgs.receive
        recorded = LavinMQ::AMQP::Consumer.capacity_on_recover = [] of Bool
        ch.basic_recover(requeue: false)
        msgs.receive.redelivered.should be_true
        recorded.should eq [false]
        server_ch = s.connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        server_ch.has_capacity?.should be_false
      ensure
        LavinMQ::AMQP::Consumer.capacity_on_recover = nil
      end
    end
  end
end
