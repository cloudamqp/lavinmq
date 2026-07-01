require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

module StreamSpecHelpers
  def self.publish(s, queue_name, nr_of_messages)
    args = {"x-queue-type": "stream"}
    with_channel(s) do |ch|
      q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
      nr_of_messages.times { |i| q.publish "m#{i}" }
    end
  end

  def self.consume_one(s, queue_name, c_tag, c_args = AMQP::Client::Arguments.new)
    args = {"x-queue-type": "stream"}
    with_channel(s) do |ch|
      ch.prefetch 1
      q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
      msgs = Channel(AMQP::Client::DeliverMessage).new
      q.subscribe(no_ack: false, tag: c_tag, args: c_args) do |msg|
        msgs.send msg
        msg.ack
      end
      msgs.receive
    end
  end

  def self.offset_from_headers(headers)
    if headers
      headers["x-stream-offset"].as(Int64)
    else
      fail("No headers found")
    end
  end
end

class LavinMQ::AMQP::Stream
  def unmap_and_remove_segments_for_spec
    unmap_and_remove_segments
  end
end

describe LavinMQ::AMQP::Stream do
  stream_queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})

  describe "Consume" do
    it "should get message with offset 2" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("pub_and_consume", args: stream_queue_args)
          10.times { |i| q.publish "m#{i}" }
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 2})) do |msg|
            msgs.send msg
          end
          msg = msgs.receive
          msg.properties.headers.should eq LavinMQ::AMQP::Table.new({"x-stream-offset": 2})
          msg.body_io.to_s.should eq "m1"
        end
      end
    end

    it "should get same nr of messages as published" do
      with_amqp_server do |s|
        with_channel(s) do |ch2|
          q = ch2.queue("pub_and_consume_2", args: stream_queue_args)
          10.times { |i| q.publish "m#{i}" }
          ch2.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          10.times do
            msgs.receive
          end
        end
      end
    end

    it "should be able to consume multiple times" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("pub_and_consume_3", args: stream_queue_args)
          10.times { |i| q.publish "m#{i}" }
          msgs = Channel(AMQP::Client::DeliverMessage).new(20)
          ch.prefetch 1
          2.times do
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
              msgs.send(msg)
              msg.ack
            end
          end
          20.times do
            msgs.receive
          end
        end
      end
    end

    it "consumer gets new messages as they arrive" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-consume", args: AMQP::Client::Arguments.new(args))
          q.publish "1"
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false) do |msg|
            msg.ack
            msgs.send(msg)
          end
          msgs.receive.body_io.to_s.should eq "1"
          q.publish "2"
          msgs.receive.body_io.to_s.should eq "2"
        end
      end
    end

    it "multiple consumers get new messages immediately as they arrive" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-consume-multiple", args: AMQP::Client::Arguments.new(args))

          # Publish an initial message
          q.publish "initial"

          # Set up two consumers
          consumer1_msgs = Channel(AMQP::Client::DeliverMessage).new
          consumer2_msgs = Channel(AMQP::Client::DeliverMessage).new

          # Consumer 1 starts from the beginning
          q.subscribe(no_ack: false, tag: "consumer1", args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msg.ack
            consumer1_msgs.send(msg)
          end

          # Consumer 2 starts from the beginning as well
          q.subscribe(no_ack: false, tag: "consumer2", args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msg.ack
            consumer2_msgs.send(msg)
          end

          # Both consumers should receive the initial message
          consumer1_msgs.receive.body_io.to_s.should eq "initial"
          consumer2_msgs.receive.body_io.to_s.should eq "initial"

          # Publish a new message
          q.publish "new_message"

          # Both consumers should immediately receive the new message
          # Use select with timeout to detect if messages aren't delivered immediately
          received_count = 0
          timeout = 2.seconds

          select
          when msg1 = consumer1_msgs.receive
            msg1.body_io.to_s.should eq "new_message"
            received_count += 1
          when timeout(timeout)
            fail("Consumer 1 didn't receive new message within #{timeout}")
          end

          select
          when msg2 = consumer2_msgs.receive
            msg2.body_io.to_s.should eq "new_message"
            received_count += 1
          when timeout(timeout)
            fail("Consumer 2 didn't receive new message within #{timeout}")
          end

          received_count.should eq 2
        end
      end
    end

    it "reproduces bug: second consumer doesn't get immediate delivery" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-bug-test", args: AMQP::Client::Arguments.new(args))

          # Set up two consumers that both start from "next" (end of stream)
          consumer1_msgs = Channel(AMQP::Client::DeliverMessage).new
          consumer2_msgs = Channel(AMQP::Client::DeliverMessage).new

          # Both consumers start from the next offset (waiting for new messages)
          q.subscribe(no_ack: false, tag: "consumer1", args: AMQP::Client::Arguments.new({"x-stream-offset": "next"})) do |msg|
            msg.ack
            consumer1_msgs.send(msg)
          end

          q.subscribe(no_ack: false, tag: "consumer2", args: AMQP::Client::Arguments.new({"x-stream-offset": "next"})) do |msg|
            msg.ack
            consumer2_msgs.send(msg)
          end

          # Small delay to ensure consumers are ready
          sleep 0.1.seconds

          # Publish a new message - both consumers should receive it immediately
          q.publish "test_message"

          # Test with a short timeout - both should receive within 1 second
          timeout = 1.seconds

          # Check consumer 1
          select
          when msg1 = consumer1_msgs.receive
            msg1.body_io.to_s.should eq "test_message"
          when timeout(timeout)
            fail("Consumer 1 failed to receive new message")
          end

          # Check consumer 2
          select
          when msg2 = consumer2_msgs.receive
            msg2.body_io.to_s.should eq "test_message"
          when timeout(timeout)
            fail("Consumer 2 failed to receive new message")
          end
        end
      end
    end
  end

  it "consume from timestamp offset across segment boundary", tags: "slow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("stream-ts-across-segments", args: stream_queue_args)
        # Half-segment payload so each publish lands in its own segment.
        data = Bytes.new(LavinMQ::Config.instance.segment_size // 2)
        q.publish_confirm data
        # Sleep > 1s so the two messages land in distinct whole seconds
        sleep 1.2.seconds
        q.publish_confirm data
        # Derive target from msg1's stored timestamp; Time.utc here would race RoughTime's
        # 100ms coarsening and could land inside segment 2's bucket, hanging the consumer.
        store = s.vhosts["/"].queue("stream-ts-across-segments").as(LavinMQ::AMQP::Stream).stream_msg_store
        msg1_ts = store.@segment_last_ts.values.first
        target_time = Time.unix(msg1_ts // 1000 + 1)
        ch.prefetch 1
        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": target_time})) do |msg|
          msgs.send msg
          msg.ack
        end
        msg = msgs.receive
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 2
      end
    end
  end

  describe "x-stream-offset negative integer" do
    it "delivers the last N messages" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-last-n", args: stream_queue_args)
          200.times { |i| q.publish "m#{i}" }
          ch.prefetch 100
          msgs = Channel(AMQP::Client::DeliverMessage).new(100)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -100})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          received = Array(AMQP::Client::DeliverMessage).new
          100.times { received << msgs.receive }
          StreamSpecHelpers.offset_from_headers(received.first.properties.headers).should eq 101
          StreamSpecHelpers.offset_from_headers(received.last.properties.headers).should eq 200
        end
      end
    end

    it "continues with new messages after delivering the last N messages" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-continues", args: stream_queue_args)
          5.times { |i| q.publish "m#{i}" }
          ch.prefetch 10
          msgs = Channel(AMQP::Client::DeliverMessage).new(4)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -3})) do |msg|
            msgs.send(msg)
            msg.ack
          end

          received = Array(AMQP::Client::DeliverMessage).new
          3.times { received << msgs.receive }
          received.map { |msg| StreamSpecHelpers.offset_from_headers(msg.properties.headers) }.should eq [3_i64, 4_i64, 5_i64]
          received.map(&.body_io.to_s).should eq ["m2", "m3", "m4"]

          q.publish "m5"
          select
          when msg = msgs.receive
            StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 6
            msg.body_io.to_s.should eq "m5"
          when timeout(1.second)
            fail("Consumer did not continue with the next stream message")
          end
        end
      end
    end

    it "clamps to oldest available when stream has fewer messages than requested" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-underflow", args: stream_queue_args)
          30.times { |i| q.publish "m#{i}" }
          ch.prefetch 30
          msgs = Channel(AMQP::Client::DeliverMessage).new(30)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -100})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          received = Array(AMQP::Client::DeliverMessage).new
          30.times { received << msgs.receive }
          StreamSpecHelpers.offset_from_headers(received.first.properties.headers).should eq 1
          StreamSpecHelpers.offset_from_headers(received.last.properties.headers).should eq 30
        end
      end
    end

    it "waits for new messages from the next offset when the stream is empty" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-empty", args: stream_queue_args)
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -100})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          q.publish "m"
          msg = msgs.receive
          StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
          msg.body_io.to_s.should eq "m"
        end
      end
    end

    it "x-stream-offset=-1 delivers only the latest message" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-one", args: stream_queue_args)
          5.times { |i| q.publish "m#{i}" }
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -1})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          msg = msgs.receive
          StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 5
          msg.body_io.to_s.should eq "m4"
        end
      end
    end

    it "delivers remaining messages when older segments have been dropped" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-length": 1}
          q = ch.queue("neg-offset-after-drop", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          q.message_count.should eq 1
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": -100})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          msg = msgs.receive
          StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 3
        end
      end
    end

    it "clamps Int64::MIN to the oldest available message" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("neg-offset-min", args: stream_queue_args)
          q.publish_confirm "m"
          ch.prefetch 1
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": Int64::MIN})) do |msg|
            msgs.send(msg)
            msg.ack
          end
          msg = msgs.receive
          StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
          msg.body_io.to_s.should eq "m"
        end
      end
    end
  end

  describe "Expiration" do
    it "segments should be removed if max-length set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-length": 1}
          q = ch.queue("stream-max-length", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          q.message_count.should eq 1
        end
      end
    end

    it "does not remove a segment currently used by a stream consumer during cleanup" do
      queue_name = Random::Secure.hex
      data = Bytes.new(LavinMQ::Config.instance.segment_size)

      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: stream_queue_args)
          3.times { q.publish_confirm data }
        end

        stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
        store = stream.stream_msg_store

        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(queue_name, args: stream_queue_args)
          msgs = Channel(AMQP::Client::DeliverMessage).new(1)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msgs.send msg
          end
          msg = msgs.receive
          consumer = wait_for { stream.consumers.first?.try &.as?(LavinMQ::AMQP::StreamConsumer) }
          protected_segment = consumer.segment
          protected_segment.should_not eq store.@segments.last_key

          store.max_length = 1_i64
          stream.unmap_and_remove_segments_for_spec

          protected_file = store.@segments[protected_segment]?
          protected_file.should_not be_nil
          protected_file.not_nil!.closed?.should be_false
          msg.ack
        end
      end
    end

    it "does not remove a segment backing an in-flight stream read" do
      queue_name = Random::Secure.hex
      data = Bytes.new(LavinMQ::Config.instance.segment_size)

      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: stream_queue_args)
          3.times { q.publish_confirm data }
        end

        stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
        store = stream.stream_msg_store
        protected_segment = store.@segments.find! { |seg_id, mfile| seg_id != store.@segments.last_key && mfile.size > 4 }[0]
        protected_segment.should_not eq store.@segments.last_key
        store.max_length = 1_i64

        stream.read(protected_segment, 4u32) do |env|
          stream.unmap_and_remove_segments_for_spec
          protected_file = store.@segments[env.segment_position.segment]?
          protected_file.should_not be_nil
          protected_file.not_nil!.closed?.should be_false
        end.should be_true
      end
    end

    it "segments should be removed if max-length-bytes set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-length-bytes": 1}
          q = ch.queue("stream-max-length-bytes", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          q.message_count.should eq 1
        end
      end
    end

    it "segments should be removed if max-age set", tags: "slow" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-age": "1s"}
          q = ch.queue("stream-max-age", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          sleep 1.1.seconds
          q.publish_confirm data
          q.message_count.should eq 1
        end
      end
    end

    it "removes segments on publish if max-age policy is set", tags: "slow" do
      with_amqp_server do |s|
        s.vhosts["/"].add_policy("max", "stream-max-age-policy", "queues", {"max-age" => JSON::Any.new("1s")}, 0i8)
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-age": "1M"}
          q = ch.queue("stream-max-age-policy", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          sleep 1.1.seconds
          q.publish_confirm data
          q.message_count.should eq 1
        end
      end
    end

    it "removes segments when max-age policy is applied", tags: "slow" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-age": "1M"}
          q = ch.queue("stream-max-age-policy", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          q.message_count.should eq 2
          sleep 1.1.seconds
          s.vhosts["/"].add_policy("max", "stream-max-age-policy", "queues", {"max-age" => JSON::Any.new("1s")}, 0i8)
          # add_policy applies the policy on a spawned fiber, so poll
          should_eventually(eq 1) { q.message_count }
        end
      end
    end

    it "removes segments when max-length-bytes policy is applied" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-max-length-bytes-policy", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          q.message_count.should eq 2
          # Apply policy - should immediately trigger cleanup
          s.vhosts["/"].add_policy("mlb", "stream-max-length-bytes-policy", "queues",
            {"max-length-bytes" => JSON::Any.new(1_i64)}, 0i8)
          # add_policy applies the policy on a spawned fiber, so poll
          should_eventually(eq 1) { q.message_count }
        end
      end
    end

    it "removes segments when max-length policy is applied" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-max-length-policy", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          q.message_count.should eq 2
          # Apply policy - should immediately trigger cleanup
          s.vhosts["/"].add_policy("ml", "stream-max-length-policy", "queues",
            {"max-length" => JSON::Any.new(1_i64)}, 0i8)
          # add_policy applies the policy on a spawned fiber, so poll
          should_eventually(eq 1) { q.message_count }
        end
      end
    end

    # Regression: a `delivery-limit` policy matching a stream queue used to
    # fall through to AMQP::Queue#apply_policy_argument, which spawned a
    # drop_redelivered fiber. That fiber called the inherited
    # MessageStore#first? and dereferenced the legacy `@rfile` — which
    # streams don't maintain through `drop_segments_while` — pointing at
    # a closed mfile from a long-dropped segment. The spawn died with
    # IO::Error("Closed mfile"). Stream now rejects the policy key
    # alongside the declare-time validation.
    it "ignores delivery-limit policy on streams" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-delivery-limit-policy", args: AMQP::Client::Arguments.new(args))
          # Roll a few segments, then drop them via max-length-bytes so the
          # inherited @rfile is left dangling at a closed mfile — the same
          # state observed in the production crash.
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          s.vhosts["/"].add_policy("mlb", "stream-delivery-limit-policy", "queues",
            {"max-length-bytes" => JSON::Any.new(1_i64)}, 0i8)
          # add_policy applies the policy on a spawned fiber, so poll
          should_eventually(eq 1) { q.message_count }
          # Now apply delivery-limit. Pre-fix this spawned drop_redelivered
          # and crashed; post-fix it's skipped entirely.
          s.vhosts["/"].add_policy("dl", "stream-delivery-limit-policy", "queues",
            {"delivery-limit" => JSON::Any.new(5_i64)}, 1i8)
          stream = s.vhosts["/"].queue("stream-delivery-limit-policy").as(LavinMQ::AMQP::Stream)
          stream.effective_policy_args.should_not contain "delivery-limit"
          # Give the (non-existent post-fix) spawn fiber a chance to run and
          # verify the queue is still functional.
          Fiber.yield
          q.publish_confirm "ok"
          q.message_count.should eq 2
        end
      end
    end

    it "does not start the expire fiber when the last consumer leaves" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream"}
          q = ch.queue("stream-expire-after-drop", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          4.times { q.publish_confirm data }

          stream = s.vhosts["/"].queue("stream-expire-after-drop").as(LavinMQ::AMQP::Stream)
          # Pin the inherited @rfile onto a populated segment, then drop that
          # segment via max-length-bytes so @rfile dangles at a closed mfile —
          # the same state observed in the production crash.
          stream.@msg_store.first?
          s.vhosts["/"].add_policy("mlb", "stream-expire-after-drop", "queues",
            {"max-length-bytes" => JSON::Any.new(1_i64)}, 0i8)

          ch.prefetch 1
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "next"})) { }
          should_eventually(eq 1) { stream.consumers.size }

          # Pre-fix this ran ensure_expire_fiber -> should_start_expire_fiber? ->
          # MessageStore#first?, dereferencing the dropped+closed read segment
          # and crashing the connection's read_loop fiber. Post-fix the check is
          # skipped for streams, so removing the last consumer must not raise.
          stream.rm_consumer(stream.consumers.first)
          stream.message_expire_fiber_active?.should be_false
        end
      end
    end

    it "meta files should be removed when segment is removed" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-length": 1}
          q = ch.queue("stream-max-length", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          dir = s.vhosts["/"].queue("stream-max-length").as(LavinMQ::AMQP::Stream).@data_dir
          File.exists?(File.join(dir, "msgs.0000000001")).should be_false
          File.exists?(File.join(dir, "meta.0000000001")).should be_false
          q.message_count.should eq 1
        end
      end
    end

    it "should not lose messages on restart when max-age is set" do
      queue_name = Random::Secure.hex
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-age": "1h"}
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }
          q.message_count.should eq 3

          stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
          stream.close
          stream.restart!
          stream.message_count.should eq 3
        end
      end
    end
  end

  it "doesn't support basic_get" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("stream_basic_get", args: stream_queue_args)
        q.publish_confirm "foobar"
        expect_raises(AMQP::Client::Channel::ClosedException, /NOT_IMPLEMENTED.*basic_get/) do
          ch.basic_get(q.name, no_ack: false)
        end
      end
    end
  end

  it "drops a publish when the store is closed concurrently" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("stream-closed-store-publish", args: stream_queue_args)
        stream = s.vhosts["/"].queue("stream-closed-store-publish").as(LavinMQ::AMQP::Stream)
        # Simulate a delete racing publish_internal: the store is closed after
        # the @state.closed? check but before push, so push raises ClosedError.
        # The publish must report Dropped, not raise (mirrors the queue spec).
        stream.@msg_store.close
        msg = LavinMQ::Message.new("", "stream-closed-store-publish", "body", LavinMQ::AMQP::Properties.new)
        stream.publish(msg).dropped?.should be_true
      end
    end
  end

  it "can requeue msgs per consumer" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("stream-requeue", args: stream_queue_args)
        q.publish_confirm "foobar"
        msgs = Channel(AMQP::Client::DeliverMessage).new
        ch.prefetch 1
        q.subscribe(no_ack: false) do |msg|
          msgs.send msg
        end
        msg1 = msgs.receive
        msg1.body_io.to_s.should eq "foobar"
        msg1.reject(requeue: true)
        msg2 = msgs.receive
        msg2.body_io.to_s.should eq "foobar"
      end
    end
  end

  it "can start consume from last segment even if queue is empty" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("empty-stream", args: stream_queue_args)
        ch.prefetch 1
        q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "last"})) do |msg|
          msg.ack
        end
      end
    end
  end

  it "can be purged" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("purge-stream", args: stream_queue_args)
        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        3.times { q.publish_confirm data }
        q.purge[:message_count].should eq 2 # never deletes the last segment
      end
    end
  end

  describe "Automatic consumer offset tracking" do
    it "resumes from last offset on reconnect" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      offset = 3

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, offset + 1)
        offset.times { StreamSpecHelpers.consume_one(s, queue_name, consumer_tag) }
        sleep 0.1.seconds

        # consume again, should start from last offset automatically
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq offset + 1
      end
    end

    it "reads offsets from file on init" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, 24_i64, 1_i64, 100_i64, 42_i64]
      tag_prefix = "ctag-"

      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(vhost.data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        offsets.each_with_index do |offset, i|
          msg_store.store_consumer_offset(tag_prefix + i.to_s, offset)
        end
        msg_store.close
        wait_for { msg_store.@closed }

        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        offsets.each_with_index do |offset, i|
          msg_store.last_offset_by_consumer_tag(tag_prefix + i.to_s).should eq offset
        end
        msg_store.close
      end
    end

    it "appends consumer tag file" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, 24_i64, 1_i64, 100_i64, 42_i64]
      consumer_tag = "ctag-1"
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        offsets.each do |offset|
          msg_store.store_consumer_offset(consumer_tag, offset)
        end
        bytesize = consumer_tag.bytesize + 1 + 8
        msg_store.@consumer_offsets.size.should eq bytesize*5
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq offsets.last
        msg_store.close
      end
    end

    it "compacts consumer tag file on restart" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, 24_i64, 1_i64, 100_i64, 42_i64]
      consumer_tag = "ctag-1"
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        offsets.each do |offset|
          msg_store.store_consumer_offset(consumer_tag, offset)
        end
        msg_store.close

        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq offsets.last
        bytesize = consumer_tag.bytesize + 1 + 8
        msg_store.@consumer_offsets.size.should eq bytesize
        msg_store.close
      end
    end

    it "compacts consumer tag file when full" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, 24_i64, 1_i64, 100_i64, 42_i64]
      consumer_tag = Random::Secure.hex(32)
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        bytesize = consumer_tag.bytesize + 1 + 8

        offsets = (LavinMQ::Config.instance.segment_size / bytesize).to_i32 + 1
        offsets.times do |i|
          msg_store.store_consumer_offset(consumer_tag, i)
        end
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq offsets - 1
        msg_store.@consumer_offsets.size.should eq bytesize*2
        msg_store.close
      end
    end

    it "does not track offset if x-stream-offset is set" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 2)
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # should consume the same message again since tracking was not saved from last consume
        msg_2 = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamSpecHelpers.offset_from_headers(msg_2.properties.headers).should eq 1
      end
    end

    it "should not use saved offset if x-stream-offset is set" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 2)

        # get message without x-stream-offset, tracks offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # consume with x-stream-offset set, should consume the same message again
        msg_2 = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg_2.properties.headers).should eq 1
      end
    end

    it "should use saved offset if x-stream-offset & x-stream-automatic-offset-tracking is set" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0, "x-stream-automatic-offset-tracking": "true"})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 2)

        # tracks offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # should continue from tracked offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 2
      end
    end

    it "should not use saved offset if x-stream-automatic-offset-tracking is false" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0, "x-stream-automatic-offset-tracking": "false"})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 2)

        # does not track offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # should consume the same message again, no tracked offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
      end
    end

    it "drop_overflow does not raise after the store has been deleted" do
      queue_name = Random::Secure.hex
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.store_consumer_offset("ctag", 1_i64)
        msg_store.delete
        msg_store.drop_overflow
      end
    end

    it "store_consumer_offset raises ClosedError after the store is closed" do
      queue_name = Random::Secure.hex
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.close
        # A late ack must surface ClosedError (like find_offset/shift?/push),
        # not a raw "Closed mfile" IO::Error that escapes the read_loop.
        expect_raises(LavinMQ::MessageStore::ClosedError) do
          msg_store.store_consumer_offset("ctag", 1_i64)
        end
      end
    end

    it "acking after the queue is deleted does not tear down the connection" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-automatic-offset-tracking": "true"})
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(queue_name, args: stream_queue_args)
          msgs = Channel(AMQP::Client::DeliverMessage).new(1)
          # Hold the message unacked so we control when the ack is sent.
          q.subscribe(no_ack: false, tag: consumer_tag, args: c_args) do |msg|
            msgs.send msg
          end
          msg = msgs.receive

          # Delete the queue out from under the in-flight ack: the message store
          # is closed while the channel's Unack still references this consumer.
          s.vhosts["/"].queue(queue_name).delete

          # The late ack stores the offset on a closed store, which raises
          # MessageStore::ClosedError. StreamConsumer#ack must swallow it so the
          # exception doesn't escape basic_ack and tear down the read_loop.
          msg.ack

          # Connection/read_loop survives: a follow-up RPC on the same channel
          # still round-trips. Without the rescue this raises (connection gone).
          ch.queue(Random::Secure.hex, args: stream_queue_args).should_not be_nil
        end
      end
    end

    it "negative x-stream-offset does not override tracked offset on reconnect" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": -5, "x-stream-automatic-offset-tracking": "true"})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 10)
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 6
        sleep 0.1.seconds

        StreamSpecHelpers.publish(s, queue_name, 5)

        # tracked offset (7) wins over the -5 that would otherwise re-anchor to 11
        msg2 = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg2.properties.headers).should eq 7
      end
    end

    it "cleanup_consumer_offsets removes outdated offset" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, -10_i64]
      tag_prefix = "ctag-"

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        offsets.each_with_index do |offset, i|
          msg_store.store_consumer_offset(tag_prefix + i.to_s, offset)
        end
        sleep 0.1.seconds
        msg_store.cleanup_consumer_offsets
        msg_store.close
        sleep 0.1.seconds

        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(tag_prefix + 1.to_s).should eq nil
        msg_store.last_offset_by_consumer_tag(tag_prefix + 0.to_s).should eq offsets[0]
        msg_store.close
      end
    end

    it "cleanup_consumer_offsets does not overflow with many consumer offsets" do
      queue_name = Random::Secure.hex
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)

        # Enough fixed-width entries that the summed size exceeds
        # Int32::MAX // 1000 (~2.1 MB), which overflowed the old `capacity * 1000`.
        tag_prefix = "c" * 240
        entry_size = (tag_prefix.bytesize + 5) + 1 + 8
        count = Int32::MAX // 1000 // entry_size + 2
        count.times do |i|
          msg_store.store_consumer_offset("#{tag_prefix}#{i.to_s.rjust(5, '0')}", i.to_i64)
        end

        msg_store.cleanup_consumer_offsets # raised OverflowError before the fix
        msg_store.close
      end
    end

    it "ConsumerOffsets.trim_to_size drops the oldest offsets when over the cap" do
      # {consumer_tag, offset, file_position}; higher position == more recent.
      # Each entry is 6 + 1 + 8 = 15 bytes.
      tracked_offsets = [
        {"oldest", 1_i64, 0_i64},
        {"middle", 2_i64, 100_i64},
        {"newest", 3_i64, 200_i64},
      ]
      # 30-byte cap fits one entry (`>=` stays strictly below), keeps newest only.
      kept = LavinMQ::AMQP::ConsumerOffsets.trim_to_size(tracked_offsets, max_size: 30_i64)
      kept.map(&.first).should eq ["newest"]

      # Larger cap keeps more, oldest-first, still dropping the oldest.
      kept = LavinMQ::AMQP::ConsumerOffsets.trim_to_size(tracked_offsets, max_size: 45_i64)
      kept.map(&.first).should eq ["middle", "newest"]
    end

    it "runs cleanup when removing segment" do
      consumer_tag = "ctag-1"
      queue_name = Random::Secure.hex
      args = {"x-queue-type": "stream", "x-max-length": 1}
      msg_body = Bytes.new(LavinMQ::Config.instance.segment_size)

      with_amqp_server do |s|
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)

        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          q.publish_confirm msg_body
        end

        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, tag: consumer_tag) do |msg|
            msgs.send msg
            msg.ack
          end
          msgs.receive
        end

        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq 2

        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          2.times { q.publish_confirm msg_body }
        end

        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq nil
      end
    end

    it "does not track offset if c-tag is auto-generated" do
      queue_name = Random::Secure.hex

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)
        args = {"x-queue-type": "stream"}
        c_tag = ""
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          msgs = Channel(AMQP::Client::DeliverMessage).new
          c_tag = q.subscribe(no_ack: false) do |msg|
            msgs.send msg
            msg.ack
          end
          msgs.receive
        end

        sleep 0.1.seconds
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(c_tag).should eq nil
      end
    end

    it "expands consumer offset file when needed" do
      queue_name = Random::Secure.hex
      consumer_tag_prefix = Random::Secure.hex(32)
      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 1)
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        one_offset_bytesize = "#{consumer_tag_prefix}1000".bytesize + 1 + 8
        offsets = (LavinMQ::Config.instance.segment_size / one_offset_bytesize).to_i32 + 1
        bytesize = 0
        offsets.times do |i|
          consumer_tag = "#{consumer_tag_prefix}#{i + 1000}"
          msg_store.store_consumer_offset(consumer_tag, i + 1000)
          bytesize += consumer_tag.bytesize + 1 + 8
        end
        msg_store.@consumer_offsets.size.should eq bytesize
        msg_store.@consumer_offsets.size.should be > LavinMQ::Config.instance.segment_size
        offsets.times do |i|
          msg_store.last_offset_by_consumer_tag("#{consumer_tag_prefix}#{i + 1000}").should eq i + 1000
        end
      end
    end
  end

  describe "Abrupt consumer disconnect" do
    # Regression: an abrupt TCP close while the server was mid-body-write made
    # `Client#deliver` rescue the IO::Error and return false silently. The
    # stream `deliver_loop` ignored that return value, so `consumer.pos` had
    # advanced past the message even though `file.pos` was still inside the
    # body. If the loop then ran another `shift?` before the connection-close
    # cleanup closed the consumer, it decoded body bytes as a message header
    # and the queue was closed with "Invalid property flags". The fix is to
    # propagate the Bool from `Client#deliver` through `Channel#deliver` and
    # `Consumer#deliver`, then have `StreamConsumer#deliver_loop` raise
    # ClosedError on a `false` return so its `ensure` block closes the FD.
    #
    # This test exercises the IO::Error path with multi-frame bodies; whether
    # the drifted `shift?` actually runs depends on a small scheduler race
    # between cleanup and the deliver_loop's next iteration. Either way, the
    # post-fix queue must stay open and continue serving consumers.
    it "stream queue survives an abrupt TCP close mid-delivery" do
      with_amqp_server do |s|
        # Body spans multiple AMQP body frames (default frame_max ~128 KiB) so
        # that an IO::Error mid-write leaves `file.pos` short of `consumer.pos`
        # — the precondition for the drift. With single-frame bodies,
        # `read_fully` completes before the failing flush and the positions
        # stay in sync.
        body = Bytes.new(512 * 1024)
        with_channel(s) do |ch|
          q = ch.queue("stream-abrupt-close", args: stream_queue_args)
          200.times { q.publish body }
        end

        20.times do
          conn = AMQP::Client.new(port: amqp_port(s), name: "abrupt-close-spec").connect
          ch = conn.channel
          ch.prefetch 50
          q = ch.queue("stream-abrupt-close", args: stream_queue_args)
          delivered = Atomic(Int32).new(0)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msg.ack rescue nil
            delivered.add(1, :relaxed)
          end
          wait_for { delivered.get(:relaxed) >= 5 }
          # SO_LINGER=0 turns close() into a TCP RST so the server's pending
          # body write fails immediately with IO::Error rather than letting
          # the kernel drain its send buffer first.
          if tcp = conn.@io.as?(TCPSocket)
            tcp.linger = 0
          end
          conn.@io.close rescue nil
        end

        # Let the server's reader fibers observe the dropped sockets.
        sleep 0.2.seconds

        stream = s.vhosts["/"].queue("stream-abrupt-close").as(LavinMQ::AMQP::Stream)
        stream.closed?.should be_false

        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream-abrupt-close", args: stream_queue_args)
          msgs = ::Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, tag: "verifier",
            args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msgs.send(msg) rescue nil
            msg.ack
          end
          select
          when msgs.receive
          when timeout(5.seconds)
            fail "stream queue stopped delivering after abrupt consumer disconnects"
          end
        end
      end
    end
  end

  describe "Restarting stream" do
    queue_name = Random::Secure.hex
    it "should restart a closed stream" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          args = {"x-queue-type": "stream"}
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
          q.publish_confirm "test message"
          stream.message_count.should eq 1

          # Close the stream
          stream.close
          stream.closed?.should be_true

          # Restart the stream & verify
          stream.restart!
          stream.closed?.should be_false
          stream.message_count.should eq 1

          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
            msgs.send msg
            msg.ack
          end
          if msg = msgs.receive
            msg.body_io.to_s.should eq "test message"
          else
            fail("Did not receive message after stream restart")
          end
        end
      end
    end

    it "should resume consuming from the correct position after a restart" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0, "x-stream-automatic-offset-tracking": "true"})

      with_amqp_server do |s|
        StreamSpecHelpers.publish(s, queue_name, 2)

        # tracks offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1

        stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
        stream.close
        stream.closed?.should be_true
        stream.restart!
        stream.closed?.should be_false

        # should continue from tracked offset
        msg = StreamSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamSpecHelpers.offset_from_headers(msg.properties.headers).should eq 2
      end
    end

    it "loads when trailing segment has only the 4-byte schema header" do
      with_datadir do |data_dir|
        # Push 2 large msgs so segment 1 fills and segment 2 is opened.
        store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        2.times { store.push(msg) }
        store.@segments.size.should be >= 2
        last_seg_path = store.@segments.last_value.path
        store.close

        # Simulate "killed after open_new_segment but before first message":
        # truncate the trailing segment to its 4-byte schema header and remove
        # its meta file so produce_metadata runs on reload.
        File.open(last_seg_path, "r+", &.truncate(4))
        meta_path = last_seg_path.sub("msgs.", "meta.")
        File.delete(meta_path) if File.exists?(meta_path)

        # Reload must not raise IndexError on the empty trailing segment.
        store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil)
        last_seg_id = store.@segments.last_key
        store.@segment_msg_count[last_seg_id].should eq 0
        store.push(msg) # the trailing segment should still be writable
        store.@segment_msg_count[last_seg_id].should eq 1
        store.close
      end
    end
  end
end
