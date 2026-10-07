require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

describe LavinMQ::AMQP::Stream do
  describe "Filters" do
    it "should only get message matching filter" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = PublishHelper.create_queue_and_messages(s, ch)

          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-filter": "foo"})) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo"
        end
      end
    end

    it "should ignore messages with non-matching filters" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = PublishHelper.create_queue_and_messages(s, ch)

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
          q = PublishHelper.create_queue_and_messages(s, ch)

          msgs = Channel(AMQP::Client::DeliverMessage).new
          filters = "foo,bar"
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
            {"x-stream-filter": filters}
          )) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar,xyz"
        end
      end
    end

    it "should get messages without filter when x-stream-match-unfiltered set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = PublishHelper.create_queue_and_messages(s, ch)

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
          msg.body_io.to_s.should eq "msg without filter"
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo"
        end
      end
    end

    it "should still respect offset values while filtering" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = PublishHelper.create_queue_and_messages(s, ch)

          msgs = Channel(AMQP::Client::DeliverMessage).new
          args = AMQP::Client::Arguments.new({"x-stream-filter": "foo", "x-stream-offset": 3})
          q.subscribe(no_ack: false, args: args) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          if headers = msg.properties.headers
            headers["x-stream-offset"].should eq 4
          else
            fail "No headers in message"
          end
        end
      end
    end

    it "applies negative offset before filtering" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = PublishHelper.create_queue_and_messages(s, ch)

          msgs = Channel(AMQP::Client::DeliverMessage).new
          args = AMQP::Client::Arguments.new({"x-stream-filter": "foo", "x-stream-offset": -2})
          q.subscribe(no_ack: false, args: args) do |msg|
            msgs.send msg
            msg.ack
          end
          msg = msgs.receive
          msg.body_io.to_s.should eq "msg with filter: foo,bar,xyz"
          if headers = msg.properties.headers
            headers["x-stream-offset"].should eq 5
          else
            fail "No headers in message"
          end
        end
      end
    end

    describe "Filter on any header" do
      it "should support filtering on any header" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = PublishHelper.create_queue_and_messages(s, ch)

            msgs = Channel(AMQP::Client::DeliverMessage).new
            filters = {"x-foo" => "bar"}
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
              {"x-stream-filter": filters}
            )) do |msg|
              msgs.send msg
              msg.ack
            end
            msg = msgs.receive
            msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          end
        end
      end

      it "x-stream-filter can be a AMQ::Protocol::Table with hashes" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = PublishHelper.create_queue_and_messages(s, ch)

            msgs = Channel(AMQP::Client::DeliverMessage).new
            filters = AMQ::Protocol::Table.new({"x-stream-filter" => "foo,bar", "x-foo" => "bar"})
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
              {"x-stream-filter": filters}
            )) do |msg|
              msgs.send msg
              msg.ack
            end
            msg = msgs.receive
            msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          end
        end
      end

      it "x-stream-filter can be an array with string and hashes" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = PublishHelper.create_queue_and_messages(s, ch)

            msgs = Channel(AMQP::Client::DeliverMessage).new
            filters = ["foo,bar", {"x-foo" => "bar"}]
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
              {"x-stream-filter": filters}
            )) do |msg|
              msgs.send msg
              msg.ack
            end
            msg = msgs.receive
            msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          end
        end
      end
    end

    describe "Handle x-filter-match-type" do
      it "ANY" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = PublishHelper.create_queue_and_messages(s, ch)

            msgs = Channel(AMQP::Client::DeliverMessage).new
            filters = "foo,bar"
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
              {"x-stream-filter": filters, "x-filter-match-type": "ANY"}
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
      it "ALL" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = PublishHelper.create_queue_and_messages(s, ch)

            msgs = Channel(AMQP::Client::DeliverMessage).new
            filters = "foo,bar"
            q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new(
              {"x-stream-filter": filters, "x-filter-match-type": "ALL"}
            )) do |msg|
              msgs.send msg
              msg.ack
            end
            msg = msgs.receive
            msg.body_io.to_s.should eq "msg with filter: foo,bar and x-foo=>bar"
          end
        end
      end
    end
  end
end

module PublishHelper
  def self.create_queue_and_messages(s, ch)
    ch.prefetch 1
    q = ch.queue("stream_filters", args: LavinMQ::AMQP::Table.new({"x-queue-type": "stream"}))
    q.publish("msg without filter")
    hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo"})
    q.publish("msg with filter: foo", props: AMQP::Client::Properties.new(headers: hdrs))
    hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "bar"})
    q.publish("msg with filter: bar", props: AMQP::Client::Properties.new(headers: hdrs))
    hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo,bar", "x-foo" => "bar"})
    q.publish("msg with filter: foo,bar and x-foo=>bar", props: AMQP::Client::Properties.new(headers: hdrs))
    hdrs = AMQP::Client::Arguments.new({"x-stream-filter-value" => "foo,bar,xyz"})
    q.publish("msg with filter: foo,bar,xyz", props: AMQP::Client::Properties.new(headers: hdrs))
    q.publish("msg without filter")
    q
  end
end

private def consumer_filter(args = NamedTuple.new)
  LavinMQ::AMQP::ConsumerFilter.from_arguments(AMQ::Protocol::Table.new(args))
end

describe LavinMQ::AMQP::ConsumerFilter do
  foo = LavinMQ::AMQP::Table.new({"x-stream-filter-value": "foo"})
  foo_bar = LavinMQ::AMQP::Table.new({"x-stream-filter-value": "foo,bar"})
  bar = LavinMQ::AMQP::Table.new({"x-stream-filter-value": "bar"})
  unfiltered = LavinMQ::AMQP::Table.new({"other": "x"})

  it "matches everything without filters" do
    filter = consumer_filter
    filter.match?(foo).should be_true
    filter.match?(unfiltered).should be_true
    filter.match?(nil).should be_true
  end

  it "requires all filters to match by default" do
    filter = consumer_filter({"x-stream-filter": "foo,bar"})
    filter.match?(foo_bar).should be_true
    filter.match?(foo).should be_false
    filter.match?(nil).should be_false
  end

  it "requires any filter to match with x-filter-match-type any" do
    filter = consumer_filter({"x-stream-filter": "foo,bar", "x-filter-match-type": "ANY"})
    filter.match?(foo).should be_true
    filter.match?(bar).should be_true
    filter.match?(unfiltered).should be_false
  end

  it "matches messages without a filter value with x-stream-match-unfiltered" do
    filter = consumer_filter({"x-stream-filter": "foo", "x-stream-match-unfiltered": true})
    filter.match?(foo).should be_true
    filter.match?(bar).should be_false
    filter.match?(unfiltered).should be_true
    filter.match?(nil).should be_true
  end

  it "rejects invalid arguments" do
    expect_raises(LavinMQ::Error::PreconditionFailed, /x-filter-match-type/) do
      consumer_filter({"x-filter-match-type": "some"})
    end
    expect_raises(LavinMQ::Error::PreconditionFailed, /x-stream-match-unfiltered/) do
      consumer_filter({"x-stream-match-unfiltered": "yes"})
    end
    expect_raises(LavinMQ::Error::PreconditionFailed, /x-stream-filter/) do
      consumer_filter({"x-stream-filter": 1})
    end
  end
end
