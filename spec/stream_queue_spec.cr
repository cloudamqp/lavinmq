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

    it "segments should be removed if max-age set" do
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

    it "removes segments on publish if max-age policy is set" do
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

    it "removes segments when max-age policy is applied" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = {"x-queue-type": "stream", "x-max-age": "1M"}
          q = ch.queue("stream-max-age-policy", args: AMQP::Client::Arguments.new(args))
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          2.times { q.publish_confirm data }
          q.message_count.should eq 2
          sleep 1.1.seconds
          s.vhosts["/"].add_policy("max", "stream-max-age-policy", "queues", {"max-age" => JSON::Any.new("1s")}, 0i8)
          q.message_count.should eq 1
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
          dir = s.vhosts["/"].queues["stream-max-length"].as(LavinMQ::AMQP::Stream).@data_dir
          File.exists?(File.join(dir, "msgs.0000000001")).should be_false
          File.exists?(File.join(dir, "meta.0000000001")).should be_false
          q.message_count.should eq 1
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
end
