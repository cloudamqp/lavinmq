require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

describe LavinMQ::AMQP::StreamQueue do
  stream_queue_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})

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
          q = ch.queue(Random::Secure.hex, args: stream_queue_args)
          q.publish("msg without filter")
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
          q.publish("msg with filter: foo", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "xyz"})
          #q.publish("msg with filter: xyz", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo,bar"})
          q.publish("msg with filter: foo,bar", props: AMQP::Client::Properties.new(headers: hdrs))
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo,bar,xyz"})
          q.publish("msg with filter: foo,bar,xyz", props: AMQP::Client::Properties.new(headers: hdrs))
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
          msg.body_io.to_s.should eq "msg with filter: foo,bar"
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar,xyz"
        end
      end
    end

    it "should support filtering on any header" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(Random::Secure.hex, args: stream_queue_args)
          q.publish("msg without filter")
          hdrs = AMQP::Client::Arguments.new({"x-foo" => "bar"})
          q.publish("msg with filter: x-foo => bar", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")
          
          msgs = Channel(AMQP::Client::DeliverMessage).new
          filters = {"x-foo" => "bar"}
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
            {"x-stream-filter": filters}
          )) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: x-foo => bar"
        end
      end
    end


    it "should support x-stream-filter and any header" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue(Random::Secure.hex, args: stream_queue_args)
          q.publish("msg without filter")
          hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo,bar", "x-foo" => "bar"})
          q.publish("msg with filter: foo,bar", props: AMQP::Client::Properties.new(headers: hdrs))
          q.publish("msg without filter")
          
          msgs = Channel(AMQP::Client::DeliverMessage).new
          filters = AMQ::Protocol::Table.new(["foo,bar", {"x-foo" => "bar"}])
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
            {"x-stream-filter": filters}
          )) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar"
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

    it "filter-match-type" do
      
    end
  end
end
