require "./spec_helper"
require "../src/lavinmq/queue_filter/source_stamp.cr"

describe LavinMQ::QueueFilter::SourceStamp do
  it "stamps required headers" do
    props = AMQ::Protocol::Properties.new
    stamped = LavinMQ::QueueFilter::SourceStamp.stamp(
      props, "src-q", "amq.direct", "rk1",
      rule_id: "r1", delivery_count: 0)
    h = stamped.headers.not_nil!
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_QUEUE].should eq "src-q"
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_EXCHANGE].should eq "amq.direct"
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_ROUTING_KEY].should eq "rk1"
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_RULE_ID].should eq "r1"
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_TIMESTAMP].should_not be_nil
    h.has_key?(LavinMQ::QueueFilter::SourceStamp::HEADER_DELIVERY_COUNT).should be_false
  end

  it "stamps delivery_count when greater than zero" do
    props = AMQ::Protocol::Properties.new
    stamped = LavinMQ::QueueFilter::SourceStamp.stamp(props, "q", "ex", "rk",
      delivery_count: 4)
    stamped.headers.not_nil![LavinMQ::QueueFilter::SourceStamp::HEADER_DELIVERY_COUNT].as(Int).should eq 4
  end

  it "omits rule_id when not provided" do
    props = AMQ::Protocol::Properties.new
    stamped = LavinMQ::QueueFilter::SourceStamp.stamp(props, "q", "ex", "rk")
    stamped.headers.not_nil!.has_key?(LavinMQ::QueueFilter::SourceStamp::HEADER_RULE_ID).should be_false
  end

  it "preserves existing user headers" do
    props = AMQ::Protocol::Properties.new
    props.headers = AMQ::Protocol::Table.new({"x-test" => "value", "user-key" => "abc"})
    stamped = LavinMQ::QueueFilter::SourceStamp.stamp(props, "q", "ex", "rk")
    h = stamped.headers.not_nil!
    h["x-test"].should eq "value"
    h["user-key"].should eq "abc"
    h[LavinMQ::QueueFilter::SourceStamp::HEADER_QUEUE].should eq "q"
  end

  it "does not mutate the source Properties' headers" do
    props = AMQ::Protocol::Properties.new
    props.headers = AMQ::Protocol::Table.new({"x-test" => "value"})
    LavinMQ::QueueFilter::SourceStamp.stamp(props, "q", "ex", "rk")
    props.headers.not_nil!.has_key?(LavinMQ::QueueFilter::SourceStamp::HEADER_QUEUE).should be_false
    props.headers.not_nil!["x-test"].should eq "value"
  end

  it "produces a Properties that round-trips other AMQP fields" do
    props = AMQ::Protocol::Properties.new(
      content_type: "application/json",
      delivery_mode: 2_u8,
      priority: 7_u8,
      correlation_id: "c1",
      message_id: "m1",
      app_id: "app",
    )
    stamped = LavinMQ::QueueFilter::SourceStamp.stamp(props, "q", "ex", "rk")
    stamped.content_type.should eq "application/json"
    stamped.delivery_mode.should eq 2_u8
    stamped.priority.should eq 7_u8
    stamped.correlation_id.should eq "c1"
    stamped.message_id.should eq "m1"
    stamped.app_id.should eq "app"
  end
end
