require "./spec_helper"

# Regression specs for https://github.com/cloudamqp/lavinmq/issues/2206
describe "Channel unacked" do
  # Row 1
  it "recovers to a live consumer without killing the connection" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish "m1"
        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false) { |m| msgs.send m }
        msgs.receive
        ch.basic_recover(requeue: false)
        msgs.receive.redelivered.should be_true
      end
    end
  end

  # Row 2
  it "treats tx ack-all with nothing outstanding as a no-op" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.tx_select
        ch.basic_ack(0_u64, multiple: true)
        ch.tx_commit
        q = ch.queue # the connection must still be usable
        q.publish "m1"
        ch.tx_commit
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m1"
      end
    end
  end

  # Row 3
  it "keeps a tx-pending delivery outstanding across basic.recover" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("txrec")
        q.publish "m1"
        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false) { |m| msgs.send m }
        msg = msgs.receive
        ch.tx_select
        ch.basic_ack(msg.delivery_tag)
        ch.basic_recover(requeue: true) # must not drop the tx-pending tag
        ch.tx_commit                    # applies tx acks before sending CommitOk
        sq = s.vhosts["/"].queues.find! &.name.== "txrec"
        sq.unacked_count.should eq 0
        sq.message_count.should eq 0
      end
    end
  end

  # Row 4
  it "takes capacity away when global prefetch is lowered" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.prefetch 10, true
        q = ch.queue
        3.times { q.publish "m" }
        msgs = [] of AMQP::Client::DeliverMessage
        q.subscribe(no_ack: false) { |msg| msgs << msg }
        wait_for { msgs.size == 3 }
        ch.prefetch 1, true
        channel = s.vhosts["/"].connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        channel.has_capacity?.should be_false
        channel.has_capacity.value.should be_false
      end
    end
  end

  # Row 5
  it "does not count basic.get deliveries against the global window" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.prefetch 1, true
        q = ch.queue
        q.publish "get-me"
        q.get(no_ack: false).should_not be_nil # one unacked, via basic.get
        delivered = Channel(String).new
        returned = Channel(String).new
        q.subscribe(no_ack: false) { |msg| delivered.send msg.body_io.to_s }
        ch.on_return { |msg| returned.send msg.body_io.to_s }
        ch.basic_publish "now", "", q.name, immediate: true
        select
        when body = delivered.receive
          body.should eq "now"
        when returned.receive
          fail "immediate publish returned though the consumer had capacity"
        when timeout 2.seconds
          fail "neither delivered nor returned"
        end
      end
    end
  end

  # Row 7
  it "lists each unacked delivery once when two consumers share a channel" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("shared")
        2.times { q.publish "m" }
        q.subscribe(no_ack: false) { }
        q.subscribe(no_ack: false) { }
        sq = s.vhosts["/"].queue("shared")
        wait_for { sq.unacked_count == 2 }
        sq.unacked_messages.size.should eq 2
      end
    end
  end

  # Row 8
  it "tears the channel down at the broker when a consumer times out" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new({"x-consumer-timeout": 1})
        q = ch.queue("timeout", args: args)
        q.publish "m"
        q.subscribe(no_ack: false) { }
        sq = s.vhosts["/"].queue("timeout")
        wait_for { sq.unacked_count == 1 }
        channel = s.vhosts["/"].connections.first.channels.first.as(LavinMQ::AMQP::Channel)
        delivered_at = channel.unacked.to_a.first.delivered_at
        wait_for { RoughTime.instant - delivered_at > 1.millisecond } # RoughTime ticks every 100ms
        channel.check_consumer_timeout
        # torn down at the broker without waiting for the client's CloseOk
        channel.running?.should be_false
        sq.unacked_count.should eq 0
        sq.message_count.should eq 1
        channel.check_consumer_timeout # a second sweep must be a no-op
      end
    end
  end
end
