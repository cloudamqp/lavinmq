require "./spec_helper"
require "./../src/lavinmq/queue"

describe LavinMQ::StreamQueue do
  opts = {"x-queue-type" => "stream"}
  tbl = LavinMQ::AMQP::Table.new(opts)

  describe "Consume" do
    it "should get message with offset 2" do
      q_name = "pub_and_consume"
      header = LavinMQ::AMQP::Table.new({"x-stream-offset" => 2})
      with_channel do |ch|
        q = ch.queue(q_name, args: tbl)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset" => 2})
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
        q = ch2.queue(q_name, args: tbl)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch2.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset" => 0})

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
        q = ch.queue(q_name, args: tbl)
        10.times do |i|
          q.publish "m#{i}"
        end

        ch.prefetch 1
        2.times do
          subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset" => 0})
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
    # it "messages should expire with time" do
    # end

    it "messages should be removed if max-length set" do
      q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream", "x-max-length": 1})
      q_name = "pub_and_consume_4"
      count = 0
      with_channel do |ch|
        q = ch.queue(q_name, args: q_args)
        5.times do |i|
          q.publish_confirm "m#{i}"
        end
        queue_stats = ch.queue_declare(q_name, passive: true)
        queue_stats[:message_count].should eq 1
      end

      with_channel do |ch|
        q = ch.queue(q_name, passive: true)
        ch.prefetch 1
        q.subscribe(args: AMQP::Client::Arguments.new({"x-stream-offset": 0})) do |msg|
          count += 1
          msg.ack
        end
        sleep 0.1
        count.should eq 1
      end
    end
  end
end
