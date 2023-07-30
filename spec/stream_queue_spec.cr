require "./spec_helper"
require "./../src/lavinmq/queue"

describe LavinMQ::StreamQueue do
  stream_queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})

  describe "Consume" do
    it "should get message with offset 2" do
      q_name = "pub_and_consume"
      header = LavinMQ::AMQP::Table.new({"x-stream-offset": 2})
      with_channel do |ch|
        q = ch.queue(q_name, args: stream_queue_args)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset": 2})
        tag = q.subscribe(no_ack: true, args: subscriber_args) do |msg|
          msg.properties.headers.should eq header
        end
        q.unsubscribe(tag)
      end
    end

    it "should get same nr of messages as published" do
      q_name = "pub_and_consume_2"
      count = 0
      with_channel do |ch2|
        q = ch2.queue(q_name, args: stream_queue_args)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch2.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset": 0})

        q.subscribe(args: subscriber_args) do |msg|
          count += 1
          msg.ack
        end
      end
      count.should eq 10
    end

    it "should be able to consume multiple times" do
      q_name = "pub_and_consume_3"
      count = 0
      with_channel do |ch|
        q = ch.queue(q_name, args: stream_queue_args)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch.prefetch 1
        2.times do
          subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset": 0})
          q.subscribe(args: subscriber_args) do |msg|
            count += 1
            msg.ack
          end
        end
      end
      count.should eq 20
    end
  end

  describe "Expiration" do
    it "segments should be removed if max-length set" do
      q_name = "pub_and_consume_4"
      with_channel do |ch|
        args = {"x-queue-type": "stream", "x-max-length": 1}
        q = ch.queue(q_name, args: AMQP::Client::Arguments.new(args))
        data = Bytes.new(1_000_000)
        10.times do
          q.publish_confirm data
        end
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
        20.times do
          q.publish_confirm data
        end
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
end
