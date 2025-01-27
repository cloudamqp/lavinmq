require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

module StreamQueueSpecHelpers
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

describe LavinMQ::AMQP::StreamQueue do
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

  it "can start consume from last segment even is queue is empty" do
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

  describe "Filters" do
    it "should only get message matching filter" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream_filter_1", args: stream_queue_args)
          q.publish("msg without filter")
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg with filter", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")

          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-filter": "foo"})) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter"
        end
      end
    end

    it "should ignore messages with non-matching filters" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream_filter_2", args: stream_queue_args)
          q.publish("msg without filter")
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg with filter", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "bar"})
          q.publish("msg with filter: bar", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")

          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-filter": "bar"})) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: bar"
        end
      end
    end

    it "should support multiple filters" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream_filter_3", args: stream_queue_args)
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg without filter")
          q.publish("msg with filter: foo", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "xyz"})
          q.publish("msg with filter: xyz", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "bar"})
          q.publish("msg with filter: bar", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")

          msgs = Channel(AMQP::Client::DeliverMessage).new
          filters = "foo,bar"
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
            {"x-stream-filter": filters}
          )) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo"
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: bar"
        end
      end
    end

    it "should get messages without filter when x-stream-match-unfiltered set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream_filter_4", args: stream_queue_args)
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg with filter: foo", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "bar"})
          q.publish("msg with filter: bar", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")

          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
            {
              "x-stream-filter":           "foo",
              "x-stream-match-unfiltered": true,
            }
          )) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo"
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg without filter"
        end
      end
    end

    it "should respect offset values while filtering" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("stream_filter_5", args: stream_queue_args)
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg with filter 1", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg with filter 2", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")

          msgs = Channel(AMQP::Client::DeliverMessage).new
          args = AMQP::Client::Arguments.new({"x-stream-filter": "foo", "x-stream-offset": 2})
          q.subscribe(no_ack: false, args: args) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter 2"
        end
      end
    end
  end
  describe "Automatic consumer offset tracking" do
    it "resumes from last offset on reconnect" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      offset = 3

      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, offset + 1)
        offset.times { StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag) }
        sleep 0.1.seconds

        # consume again, should start from last offset automatically
        msg = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamQueueSpecHelpers.offset_from_headers(msg.properties.headers).should eq offset + 1
      end
    end

    it "reads offsets from file on init" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, 24_i64, 1_i64, 100_i64, 42_i64]
      tag_prefix = "ctag-"

      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        StreamQueueSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(vhost.data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        offsets.each_with_index do |offset, i|
          msg_store.store_consumer_offset(tag_prefix + i.to_s, offset)
        end
        msg_store.close
        wait_for { msg_store.@closed }

        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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
        StreamQueueSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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
        StreamQueueSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        offsets.each do |offset|
          msg_store.store_consumer_offset(consumer_tag, offset)
        end
        msg_store.close

        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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
        StreamQueueSpecHelpers.publish(s, queue_name, 1)
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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
        StreamQueueSpecHelpers.publish(s, queue_name, 2)
        msg = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamQueueSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # should consume the same message again since tracking was not saved from last consume
        msg_2 = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamQueueSpecHelpers.offset_from_headers(msg_2.properties.headers).should eq 1
      end
    end

    it "should not use saved offset if x-stream-offset is set" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0})

      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, 2)

        # get message without x-stream-offset, tracks offset
        msg = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag)
        StreamQueueSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # consume with x-stream-offset set, should consume the same message again
        msg_2 = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamQueueSpecHelpers.offset_from_headers(msg_2.properties.headers).should eq 1
      end
    end

    it "should use saved offset if x-stream-offset & x-stream-automatic-offset-tracking is set" do
      queue_name = Random::Secure.hex
      consumer_tag = Random::Secure.hex
      c_args = AMQP::Client::Arguments.new({"x-stream-offset": 0, "x-stream-automatic-offset-tracking": true})

      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, 2)

        # get message without x-stream-offset, tracks offset
        msg = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamQueueSpecHelpers.offset_from_headers(msg.properties.headers).should eq 1
        sleep 0.1.seconds

        # consume with x-stream-offset set, should consume the same message again
        msg_2 = StreamQueueSpecHelpers.consume_one(s, queue_name, consumer_tag, c_args)
        StreamQueueSpecHelpers.offset_from_headers(msg_2.properties.headers).should eq 2
      end
    end

    it "cleanup_consumer_offsets removes outdated offset" do
      queue_name = Random::Secure.hex
      offsets = [84_i64, -10_i64]
      tag_prefix = "ctag-"

      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, 1)

        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        offsets.each_with_index do |offset, i|
          msg_store.store_consumer_offset(tag_prefix + i.to_s, offset)
        end
        sleep 0.1.seconds
        msg_store.cleanup_consumer_offsets
        msg_store.close
        sleep 0.1.seconds

        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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

        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq 2

        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new(args))
          2.times { q.publish_confirm msg_body }
        end

        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(consumer_tag).should eq nil
      end
    end

    it "does not track offset if c-tag is auto-generated" do
      queue_name = Random::Secure.hex

      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, 1)
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
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
        msg_store.last_offset_by_consumer_tag(c_tag).should eq nil
      end
    end

    it "expands consumer offset file when needed" do
      queue_name = Random::Secure.hex
      consumer_tag_prefix = Random::Secure.hex(32)
      with_amqp_server do |s|
        StreamQueueSpecHelpers.publish(s, queue_name, 1)
        data_dir = File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest queue_name)
        msg_store = LavinMQ::AMQP::StreamQueue::StreamQueueMessageStore.new(data_dir, nil)
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
