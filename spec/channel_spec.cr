require "./spec_helper"

class LavinMQ::AMQP::Channel
  def spec_next_delivery_tag(queue, sp, consumer)
    next_delivery_tag(queue, sp, false, consumer)
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
      end
    end
  end
end
