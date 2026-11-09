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
    it "should not deliver past a global prefetch lowered below the unacked count" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue
          5.times { q.publish "m1" }
          msgs = [] of AMQP::Client::DeliverMessage
          q.subscribe(no_ack: false) { |msg| msgs << msg }
          wait_for { msgs.size == 5 }
          ch.prefetch 2, true
          q.publish "m1"
          sleep 50.milliseconds
          msgs.size.should eq 5
          msgs.first(4).each &.ack
          wait_for { msgs.size == 6 }
        end
      end
    end

    {1, 3, 8}.each do |prefetch|
      it "should not let one consumer starve the others when global is true (prefetch #{prefetch})" do
        with_amqp_server do |s|
          queues = {"global-prefetch-q1", "global-prefetch-empty", "global-prefetch-q2"}
          with_channel(s) do |ch|
            queues.each { |name| ch.queue(name) }
            100.times do
              ch.basic_publish "1", "", "global-prefetch-q1"
              ch.basic_publish "2", "", "global-prefetch-q2"
            end
          end
          with_channel(s) do |ch|
            ch.prefetch prefetch, true
            order = Channel(String).new(200)
            queues.each do |name|
              ch.basic_consume(name, no_ack: false, block: false) do |msg|
                order.send msg.body_io.to_s
                msg.ack
              end
            end
            first = Array(String).new(100) { order.receive }
            longest_run = first.chunk_while { |a, b| a == b }.max_of(&.size)
            longest_run.should be <= 4 * prefetch
          end
        end
      end
    end
  end
end
