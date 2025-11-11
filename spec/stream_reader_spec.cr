require "./spec_helper"
require "./../src/lavinmq/amqp/stream/stream_reader"
require "./../src/lavinmq/amqp/stream/stream_message_store"

describe LavinMQ::AMQP::StreamReader do
  it "should handle offset (where to start the reader)" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("streams", "direct")
        q = ch.queue("", args: AMQP::Client::Arguments.new({
          "x-queue-type" => "stream",
        }))
        q.bind(x.name, q.name)
        10.times do |i|
          x.publish_confirm("test message #{i}", q.name)
        end

        iq = s.vhosts["/"].queues[q.name].as(LavinMQ::AMQP::Stream)
        stream = iq.reader 5

        count = 0
        stream.each do |env|
          body = String.new(env.message.body)
          body.should eq "test message #{count + 4}"
          count += 1
        end
        count.should eq 6
      end
    end
  end
  it "should read over multiple segments" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("streams", "direct")
        q = ch.queue("", args: AMQP::Client::Arguments.new({
          "x-queue-type" => "stream",
        }))
        q.bind(x.name, q.name)
        400.times do |i|
          x.publish_confirm("test message #{i}" * 100, q.name)
        end

        iq = s.vhosts["/"].queues[q.name].as(LavinMQ::AMQP::Stream)
        stream = iq.reader 0

        count = 0
        seg = 0
        stream.each do |env|
          seg = env.segment_position.segment
          count += 1
        end
        count.should eq 400
        seg.should eq 2
      end
    end
  end
end
