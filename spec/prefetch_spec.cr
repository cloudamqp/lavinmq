require "./spec_helper"

describe LavinMQ::Server do
  describe "Consumer with prefetch" do
    it "should receive prefetch number of messages" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 2
          pmsg = "m1"
          q = ch.queue
          3.times { q.publish pmsg }
          msgs = [] of AMQP::Client::DeliverMessage
          q.subscribe(no_ack: false) { |msg| msgs << msg }
          wait_for { msgs.size == 2 }
          msgs.size.should eq 2
        end
      end
    end

    it "should receive more after acks" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue
          4.times { q.publish "m1" }
          c = 0
          q.subscribe(no_ack: false) do |msg|
            c += 1
            msg.ack
          end
          wait_for { c == 4 }
          c.should eq 4
        end
      end
    end

    it "should be applied separately to each new consumer on the channel when global is false" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 2
          pmsg = "m1"
          q = ch.queue
          4.times { q.publish pmsg }
          msgs_c1 = [] of AMQP::Client::DeliverMessage
          msgs_c2 = [] of AMQP::Client::DeliverMessage
          q.subscribe(no_ack: false) { |msg| msgs_c1 << msg }
          q.subscribe(no_ack: false) { |msg| msgs_c2 << msg }
          wait_for { msgs_c2.size == 2 }
          msgs_c1.size.should eq 2
          msgs_c2.size.should eq 2
        end
      end
    end

    it "should be shared across all consumers on the channel when global is true" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 2, true
          pmsg = "m1"
          q = ch.queue
          4.times { q.publish pmsg }
          msgs_c1 = [] of AMQP::Client::DeliverMessage
          msgs_c2 = [] of AMQP::Client::DeliverMessage
          q.subscribe(no_ack: false) { |msg| msgs_c1 << msg }
          wait_for { msgs_c1.size == 2 }
          q.subscribe(no_ack: false) { |msg| msgs_c2 << msg }
          sleep 5.milliseconds
          msgs_c1.size.should eq 2
          msgs_c2.size.should eq 0
        end
      end
    end
  end

  describe "Channel with global prefetch" do
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

    it "does not count basic.get deliveries against the window" do
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
  end
end
