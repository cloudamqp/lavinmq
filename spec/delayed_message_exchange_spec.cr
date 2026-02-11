require "./spec_helper"

describe "Delayed Message Exchange" do
  x_name = "delayed-topic"
  delay_q_name = "amq.delayed-#{x_name}"
  x_args = AMQP::Client::Arguments.new({"x-delayed-exchange" => true})

  describe "internal queue" do
    it "should use old name if dir exists" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        legacy_q_name = "amq.delayed.#{x_name}"
        legacy_q_dir = Digest::SHA1.hexdigest(legacy_q_name)
        data_dir = Path[vhost.data_dir]
        Dir.mkdir data_dir / legacy_q_dir

        with_channel(s) do |ch|
          ch.exchange(x_name, "topic", args: x_args)
          q = s.vhosts["/"].queues[legacy_q_name]?
          q.should_not be_nil
        end
      end
    end

    it "should be created with x-dead-letter-exchange" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange(x_name, "topic", args: x_args)
          q = s.vhosts["/"].queues[delay_q_name]?
          q.should_not be_nil
          dlx_exchange = q.not_nil!.arguments["x-dead-letter-exchange"]?.try &.as?(String)
          dlx_exchange.should eq x_name
        end
      end
    end

    it "should deny publish via amqp (default exchange)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          # Declare exchange
          ch.exchange(x_name, "topic", args: x_args)
          ch.exchange("", delay_q_name, args: x_args)
          ch.basic_publish_confirm "test", exchange: "", routing_key: delay_q_name
          s.vhosts["/"].queues[delay_q_name].message_count.should eq 0
        end
      end
    end

    it "should rebuild index on restart" do
      with_amqp_server do |s|
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1000})
        with_channel(s) do |ch|
          ex = ch.exchange(x_name, "topic", args: x_args)
          ex.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
          s.vhosts["/"].queues[delay_q_name].message_count.should eq 1
        end
        s.restart
        s.vhosts["/"].queues[delay_q_name].message_count.should eq 1
        sleep 1.second
        wait_for { s.vhosts["/"].queues[delay_q_name].message_count == 0 }
      end
    end

    it "should rebuild index on restart with truncated segment and acked messages" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "topic", args: x_args)
          q = ch.queue("delayed_q")
          q.bind(x.name, "#")
          hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
          x.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
          wait_for { s.vhosts["/"].queues["delayed_q"].message_count == 1 }
        end
        # All messages in delayed queue are now expired and acked
        s.vhosts["/"].queues[delay_q_name].message_count.should eq 0
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(delay_q_name))

        s.stop

        # Truncate segment file mid-message, simulating a crash where
        # mmap data wasn't fully flushed to disk
        seg_file = File.join(data_dir, "msgs.0000000001")
        File.open(seg_file, "r+") { |f| f.truncate(f.size // 2) }

        s.restart
        s.vhosts["/"].queues[delay_q_name].message_count.should eq 0
      end
    end

    it "should rebuild index on restart preserving valid messages despite trailing corrupt data" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "topic", args: x_args)
          hdrs = AMQP::Client::Arguments.new({"x-delay" => 300_000})
          x.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
          s.vhosts["/"].queues[delay_q_name].message_count.should eq 1
        end
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(delay_q_name))

        s.stop

        # Append a partial message (just a non-zero timestamp, no body),
        # simulating a crash mid-write
        seg_file = File.join(data_dir, "msgs.0000000001")
        File.open(seg_file, "a") { |f| f.write_bytes(1i64, IO::ByteFormat::SystemEndian) }

        s.restart
        s.vhosts["/"].queues[delay_q_name].message_count.should eq 1
      end
    end
  end

  q_name = "delayed_q"
  it "should delay 1 message" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
        x.publish "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        queue = s.vhosts["/"].queues[q_name]
        queue.message_count.should eq 0
        wait_for { queue.message_count == 1 }
        queue.message_count.should eq 1
      end
    end
  end

  q_name = "delayed_q"
  it "should delay 2 messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
        x.publish "test message 1", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        x.publish "test message 2", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        queue = s.vhosts["/"].queues[q_name]
        queue.message_count.should eq 0
        wait_for { queue.message_count == 2 }
        queue.message_count.should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 1")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 2")
      end
    end
  end

  it "should deliver in correct order" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1000})
        x.publish "delay-long", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1})
        x.publish "delay-short", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        queue = s.vhosts["/"].queues[q_name]
        queue.message_count.should eq 0
        wait_for { queue.message_count >= 1 }
        q.get(no_ack: true).try(&.body_io.to_s).should eq("delay-short")
        wait_for { queue.message_count == 1 }
        q.get(no_ack: true).try(&.body_io.to_s).should eq("delay-long")
      end
    end
  end

  it "should deliver based on when message is to expire", tags: "slow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")
        # Publish three message with delay 9000ms, 6000ms, 3000ms
        3.downto(1) do |i|
          delay = i * 3000
          hdrs = AMQP::Client::Arguments.new({"x-delay" => delay})
          x.publish_confirm delay.to_s, "rk", props: AMQP::Client::Properties.new(headers: hdrs)
          Fiber.yield
        end
        # by sleeping 5 seconds the message with delay 3000ms should be published
        sleep 5.seconds
        # publish another message, with a delay low enough to make the message
        # being published before at least the one with 9000ms
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 1500})
        x.publish_confirm "1500", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        Fiber.yield
        # by sleeping another 2 seconds we've slept for 7s in total, meaning that
        # the message published with 6000ms should be published. Also, the new message
        # with 1500ms should be published
        sleep 2.seconds
        queue = s.vhosts["/"].queues[q_name]
        queue.message_count.should eq 3
        sleep 3.seconds # total 10, the 9000ms message should have been published
        queue.message_count.should eq 4
        expected = %w[3000 6000 1500 9000]
        expected.each do |expected_delay|
          queue.basic_get(no_ack: true) do |env|
            String.new(env.message.body).should eq expected_delay
          end
        end
      end
    end
  end

  it "should support x-delayed-message as exchange type" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        xdm_args = AMQP::Client::Arguments.new({"x-delayed-type" => "topic"})
        x = ch.exchange(x_name, "x-delayed-message", args: xdm_args)
        q = ch.queue(q_name)
        q.bind(x.name, "rk")
        hdrs = AMQP::Client::Arguments.new({"x-delay" => 5})
        x.publish "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
        queue = s.vhosts["/"].queues[q_name]
        queue.message_count.should eq 0
        wait_for(200.milliseconds) { queue.message_count == 1 }
        queue.message_count.should eq 1
      end
    end
  end

  it "should not set dead letter headers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")

        hdrs = AMQP::Client::Arguments.new({
          "x-delay"                   => 100,
          "x-dead-letter-exchange"    => "amq.fanout",
          "x-dead-letter-routing-key" => "dead-lettered",
        })
        x.publish "delay", "rk", props: AMQP::Client::Properties.new(headers: hdrs)

        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe { |msg| msgs.send msg }
        msg = msgs.receive
        headers = msg.properties.headers.should_not be_nil
        header_keys = headers.to_h.keys
        header_keys.should_not contain "x-first-death-reason"
        header_keys.should_not contain "x-first-death-queue"
        header_keys.should_not contain "x-first-death-exchange"
        header_keys.should_not contain "x-death"
      end
    end
  end

  it "should always use x-dead-letter-exchange when expiring the message" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue(q_name)
        q.bind(x.name, "#")
        ex = s.vhosts["/"].exchanges[x_name].as(LavinMQ::AMQP::Exchange)

        delayed_q = ex.@delayed_queue.should_not be_nil

        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe { |msg| msgs.send msg }

        props = AMQP::Client::Properties.new(
          headers: AMQP::Client::Arguments.new({
            "x-delay" => 100,
          }),
        )

        # "Publish" direct to the delayed queue which leaves exchange empty
        delayed_q.delay(LavinMQ::Message.new("", delayed_q.name, "foo", props))

        # Wait for the message to be expired. Just receiving it actually verifies that
        # exchange has been set to x-dead-letter-exchange
        select
        when msg = msgs.receive
          msg.exchange.should eq x_name
        when timeout 3.second
          msgs.close
          ch.close
          raise "x-dead-letter-exchange not sent, message is looping?"
        end
      end
    end
  end

  it "should prevent binding delayed exchange to its own internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        # The internal delayed queue should exist
        internal_queue = s.vhosts["/"].queues[delay_q_name]?
        internal_queue.should_not be_nil

        # Attempting to bind the delayed exchange to its internal queue should raise an error
        expect_raises(AMQP::Client::Channel::ClosedException, /Cannot bind delayed exchange/) do
          ch.queue_bind(delay_q_name, x_name, "#")
        end
      end
    end
  end

  # Verify that a message isn't routed to the internal queue
  it "should not route messages to its own internal queue even if bound" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange(x_name, "topic", args: x_args)
        # Queue that should receive messages
        q = ch.queue(q_name)
        q.bind(x.name, "#")

        # Publish a message without delay (should route normally)
        x.publish "test message", "test.routing.key"

        # Message should reach the bound queue, not create a loop
        queue = s.vhosts["/"].queues[q_name]
        wait_for { queue.message_count == 1 }
        queue.message_count.should eq 1

        # Internal delayed queue should remain empty
        internal_queue = s.vhosts["/"].queues[delay_q_name]
        internal_queue.message_count.should eq 0
      end
    end
  end
end
