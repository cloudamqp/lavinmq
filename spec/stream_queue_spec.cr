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
        2.times do |i|
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
      opts_max_length = {"x-queue-type" => "stream", "x-max-length" => 10, "durable" => true}
      tbl_max_length = LavinMQ::AMQP::Table.new(opts_max_length)
      q_name = "pub_and_consume_4"
      count = 0
      with_channel do |ch|
        q = ch.queue(q_name, args: tbl_max_length)
        ch.prefetch 1
        50.times do |i|
          q.publish_confirm "m#{i}"
        end
      end

      last_header = LavinMQ::AMQP::Table.new({"x-stream-offset" => 0})
      with_channel do |ch|
        q = ch.queue(q_name, args: tbl_max_length)
        ch.prefetch 1
        subscriber_args = AMQP::Client::Arguments.new({"x-stream-offset" => 0})
        q.subscribe(args: subscriber_args) do |msg|
          count += 1
          msg.ack
          last_header = msg.properties.headers
        end
      end
      count.should eq 10
      puts last_header
    end
  end
end
