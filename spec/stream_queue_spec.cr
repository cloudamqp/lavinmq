require "./spec_helper"
require "./../src/lavinmq/queue"

describe LavinMQ::StreamQueue do
  stream_queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})

  describe "Consume" do
    it "should get message with offset 2" do
      with_channel do |ch|
        q = ch.queue("pub_and_consume", args: stream_queue_args)
        10.times { |i| q.publish "m#{i}" }
        ch.prefetch 1
        msgs = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": 2})) do |msg|
          msgs.send msg
        end
        msg = msgs.receive
        msg.properties.headers.should eq LavinMQ::AMQP::Table.new({"x-stream-offset": 2})
      end
    end

    it "should get same nr of messages as published" do
      with_channel do |ch2|
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

    it "should be able to consume multiple times" do
      with_channel do |ch|
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

  describe "Expiration" do
    it "segments should be removed if max-length set" do
      q_name = "pub_and_consume_4"
      with_channel do |ch|
        args = {"x-queue-type": "stream", "x-max-length": 1}
        q = ch.queue(q_name, args: AMQP::Client::Arguments.new(args))
        data = Bytes.new(1_000_000)
        10.times { q.publish_confirm data }
        count = ch.queue_declare(q_name, passive: true)[:message_count]
        count.should be < 10
      end
    end

    it "segments should be removed if max-length-bytes set" do
      q_name = "pub_and_consume_4"
      with_channel do |ch|
        args = {"x-queue-type": "stream", "x-max-length-bytes": 100_000}
        q = ch.queue(q_name, args: AMQP::Client::Arguments.new(args))
        data = Bytes.new(100_000)
        20.times { q.publish_confirm data }
        count = ch.queue_declare(q_name, passive: true)[:message_count]
        count.should be < 20
      end
    end
  end

  it "doesn't support basic_get" do
    q_name = "stream_basic_get"
    with_channel do |ch|
      q = ch.queue(q_name, args: stream_queue_args)
      q.publish_confirm "foobar"
      expect_raises(AMQP::Client::Channel::ClosedException, /NOT_IMPLEMENTED.*basic_get/) do
        ch.basic_get(q_name, no_ack: false)
      end
    end
  end

  it "can requeue msgs per consumer" do
    with_channel do |ch|
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