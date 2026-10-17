require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

describe LavinMQ::Client::Channel::Consumer do
  describe "without x-priority" do
    it "should round-robin" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          msgs = Channel(Int32).new

          q.subscribe(no_ack: false) do |msg|
            msgs.send 1
            msg.ack
          end

          q.subscribe(no_ack: false) do |msg|
            msgs.send 2
            msg.ack
          end

          q.publish("priority")
          q.publish("priority")
          msgs.receive.should eq 1
          msgs.receive.should eq 2
        end
      end
    end
  end

  describe "with x-priority" do
    it "should respect priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
          msgs = Channel(AMQP::Client::DeliverMessage?).new(1)

          q.subscribe(no_ack: false) do |_|
            msgs.send nil
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send msg
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end
        end
      end
    end

    it "should send to prioritized consumer if ready" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})
          msgs = Channel(AMQP::Client::DeliverMessage?).new(1)

          q.subscribe(no_ack: false) do |_|
            msgs.send nil
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send msg
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end

          q.publish_confirm("priority")
          m = msgs.receive
          m.should be_truthy
          if m.is_a?(AMQP::Client::DeliverMessage)
            m.ack
            m.body_io.to_s.should eq "priority"
          end
        end
      end
    end

    it "should round-robin to high priority consumers" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("consumer-priority")
          ch.prefetch 1
          subscriber_args = AMQP::Client::Arguments.new({"x-priority" => 5})

          msgs = Channel(Int32).new
          # This is a sync channel to make sure each consumer
          # is "working" long enough for the other consumer
          # to retrive a message...
          work = Channel(Bool).new
          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            msgs.send 1
            work.send true
            msg.ack
          end

          q.subscribe(no_ack: false, args: subscriber_args) do |msg|
            work.receive
            msgs.send 2
            msg.ack
          end

          100.times { q.publish("priority") }
          50.times do
            msgs.receive.should eq 1
            msgs.receive.should eq 2
          end
        end
      end
    end

    it "should accept any integer as x-priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int8::MIN})) { |_| }
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int16::MAX})) { |_| }
          q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": 1i64})) { |_| }
        end

        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /out of bounds/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": Int64::MAX})) { |_| }
          end
        end
      end
    end

    it "should not accept any thing but integers as x-priority" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /must be an integer/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": "a"})) { |_| }
          end
        end
      end

      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /must be an integer/) do
            q = ch.queue
            q.subscribe(args: AMQP::Client::Arguments.new({"x-priority": 0.1})) { |_| }
          end
        end
      end
    end
  end

  # The queue wakes one consumer per message, so the consumers that stop
  # taking messages must wake the next one, or messages are stranded
  describe "wake one consumer per message" do
    it "delivers every message when the consumers are parked without a prefetch limit" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("wake-one-no-prefetch")
          received = Atomic(Int32).new(0)
          4.times do
            q.subscribe(no_ack: true) { |_| received.add(1) }
          end
          sleep 20.milliseconds # let the consumers park
          200.times { |i| q.publish "m#{i}" }
          wait_for { received.get == 200 }
          received.get.should eq 200
        end
      end
    end

    it "delivers every message when the consumers run out of prefetch in turn" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("wake-one-prefetch")
          received = Atomic(Int32).new(0)
          4.times do
            q.subscribe(no_ack: false) do |msg|
              received.add(1)
              msg.ack
            end
          end
          sleep 20.milliseconds # let the consumers park
          100.times { |i| q.publish "m#{i}" }
          wait_for { received.get == 100 }
          received.get.should eq 100
        end
      end
    end

    it "delivers to the other consumer when one is stopped by channel flow" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("wake-one-flow")
          msgs = Channel(String).new(10)
          q.subscribe(no_ack: true) { |msg| msgs.send msg.body_io.to_s }
          ch.flow(false)
          with_channel(s) do |ch2|
            ch2.queue("wake-one-flow").subscribe(no_ack: true) { |msg| msgs.send msg.body_io.to_s }
            sleep 20.milliseconds # let both consumers park
            3.times { |i| q.publish "m#{i}" }
            3.times { msgs.receive.should start_with "m" }
          end
        end
      end
    end
  end

  describe "on-demand deliver_loop" do
    it "delivers messages published after consumer connects to empty queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("on-demand-deliver-loop")
          msgs = Channel(String).new

          q.subscribe(no_ack: true) do |msg|
            msgs.send msg.body_io.to_s
          end

          q.publish("test1")
          q.publish("test2")

          msgs.receive.should eq "test1"
          msgs.receive.should eq "test2"
        end
      end
    end

    it "delivers messages already in queue when consumer connects" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("on-demand-deliver-loop-nonempty")
          q.publish("pre-existing")

          msgs = Channel(String).new
          q.subscribe(no_ack: true) do |msg|
            msgs.send msg.body_io.to_s
          end

          msgs.receive.should eq "pre-existing"
        end
      end
    end

    it "delivers requeued messages to consumers on empty queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("on-demand-deliver-loop-requeue")
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new

          q.subscribe(no_ack: false) do |msg|
            msgs.send msg
          end

          q.publish("requeue-me")
          msg = msgs.receive
          msg.body_io.to_s.should eq "requeue-me"
          msg.reject(requeue: true)

          msg2 = msgs.receive
          msg2.body_io.to_s.should eq "requeue-me"
          msg2.ack
        end
      end
    end

    it "shuts down deliver_loop after idle timeout and respawns on next message" do
      config = LavinMQ::Config.new
      config.deliver_loop_idle_timeout = 100.milliseconds
      with_amqp_server(config: config) do |s|
        with_channel(s) do |ch|
          q = ch.queue("idle-respawn")
          msgs = Channel(String).new
          q.subscribe(no_ack: true) { |msg| msgs.send msg.body_io.to_s }

          q.publish "first"
          msgs.receive.should eq "first"

          # Wait for deliver_loop to idle out
          sleep 300.milliseconds

          q.publish "after-idle"
          select
          when msg = msgs.receive
            msg.should eq "after-idle"
          when timeout 500.milliseconds
            fail "deliver_loop did not respawn after idle timeout"
          end
        end
      end
    end

    it "respawns idle consumer's deliver loop when channel close requeues a message" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          conn = AMQP::Client.new(port: amqp_port(s)).connect
          msgs = Channel(AMQP::Client::DeliverMessage).new

          q = conn.channel.queue("respawn-on-reject")
          q.subscribe(no_ack: false) { |msg| msgs.send msg }
          q.publish "test-msg"
          msgs.receive

          # Subscribes to an empty queue, so its deliver_loop won't start
          ch.queue("respawn-on-reject").subscribe(no_ack: false) { |msg| msgs.send msg }

          # Closing connection requeues the unacked msg via reject;
          # The other consumers deliver_loop must respawn to consume it
          conn.close

          select
          when msg = msgs.receive
            msg.body_io.to_s.should eq "test-msg"
          when timeout 500.milliseconds
            fail "Consumer did not receive requeued message"
          end
        end
      end
    end
  end
end
