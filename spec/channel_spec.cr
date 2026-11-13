require "./spec_helper"
require "amqp-client"

class LavinMQ::MessageStore
  class_property? fail_next_requeue = false

  # Yields before it raises, as a reject that does I/O before it fails may,
  # so that a deliver loop takes the slot the reject freed
  def requeue(sp : SegmentPosition)
    if LavinMQ::MessageStore.fail_next_requeue?
      LavinMQ::MessageStore.fail_next_requeue = false
      Fiber.yield
      raise Error.new(@wfile)
    end
    previous_def
  end
end

# Nacks the first five deliveries of a queue with 20 messages, while the
# consumer's deliver loop waits for the channel's unacked lock, and fails the
# first requeue. The queue is paused, and the message stays in it.
private def nack_multiple_on_store_error(s, tx = false)
  with_channel(s) do |ch|
    ch.prefetch(5)
    q = ch.queue("nack-store-error")
    with_channel(s) { |pub| 20.times { pub.basic_publish_confirm "x", "", q.name } }
    tags = Channel(UInt64).new(100)
    q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
    5.times { tags.receive }
    ch.tx_select if tx
    LavinMQ::MessageStore.fail_next_requeue = true
    ch.basic_nack(5u64, requeue: true, multiple: true)
    ch.tx_commit if tx
    queue = s.vhosts["/"].queue("nack-store-error")
    should_eventually(be_true) { queue.state.paused? }
    queue.closed?.should be_false
    ch.closed?.should be_false
    # A deliver loop can have taken a message before the pause
    should_eventually(eq 20) { queue.message_count + queue.unacked_count }
    queue.resume!
    5.times { tags.receive } # the prefetch is filled again
  end
ensure
  LavinMQ::MessageStore.fail_next_requeue = false
end

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

  it "should requeue a basic_get message when the channel closes during the get" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("basic-get-close")
        q.publish_confirm("m")
        sq = s.vhosts["/"].queue("basic-get-close")
        server_ch = s.connections.first.as(LavinMQ::AMQP::Client).channels.first.as(LavinMQ::AMQP::Channel)
        got = Channel(Nil).new
        # Hold the queue's store lock so the get waits for it, then close the
        # channel while it waits, as the management API can
        sq.@msg_store_lock.synchronize do
          spawn do
            q.get(no_ack: false)
          rescue AMQP::Client::Channel::ClosedException
          ensure
            got.send nil
          end
          wait_for { sq.@get_count.get(:relaxed) == 1 }
          server_ch.close("closed during basic.get")
        end
        got.receive
        sq.unacked_count.should eq 0
        sq.message_count.should eq 1
        ch.@connection.closed?.should be_false
      end
    end
  end

  it "pauses a queue on a store error during a nack multiple without deadlocking" do
    with_amqp_server { |s| nack_multiple_on_store_error(s) }
  end

  it "pauses a queue on a store error during a tx.commit without deadlocking" do
    with_amqp_server { |s| nack_multiple_on_store_error(s, tx: true) }
  end
end
