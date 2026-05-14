require "./spec_helper"
require "../src/lavinmq/poison_pill/source_stamp.cr"

describe LavinMQ::PoisonPill::SourceStamp do
  it "stamps required headers" do
    props = AMQ::Protocol::Properties.new
    stamped = LavinMQ::PoisonPill::SourceStamp.stamp(props, "src-q", "amq.direct", "rk1")
    h = stamped.headers.not_nil!
    h[LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_QUEUE].should eq "src-q"
    h[LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_EXCHANGE].should eq "amq.direct"
    h[LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_ROUTING_KEY].should eq "rk1"
    h[LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_TIMESTAMP].should_not be_nil
    h.has_key?(LavinMQ::PoisonPill::SourceStamp::HEADER_DELIVERY_COUNT).should be_false
  end

  it "stamps delivery_count when greater than zero" do
    props = AMQ::Protocol::Properties.new
    stamped = LavinMQ::PoisonPill::SourceStamp.stamp(props, "q", "ex", "rk", delivery_count: 5)
    stamped.headers.not_nil![LavinMQ::PoisonPill::SourceStamp::HEADER_DELIVERY_COUNT].as(Int).should eq 5
  end

  it "preserves existing user headers" do
    props = AMQ::Protocol::Properties.new
    props.headers = AMQ::Protocol::Table.new({"x-test" => "value", "user-key" => "abc"})
    stamped = LavinMQ::PoisonPill::SourceStamp.stamp(props, "q", "ex", "rk")
    h = stamped.headers.not_nil!
    h["x-test"].should eq "value"
    h["user-key"].should eq "abc"
    h[LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_QUEUE].should eq "q"
  end

  it "does not mutate source Properties' headers" do
    props = AMQ::Protocol::Properties.new
    props.headers = AMQ::Protocol::Table.new({"x-test" => "value"})
    LavinMQ::PoisonPill::SourceStamp.stamp(props, "q", "ex", "rk")
    props.headers.not_nil!.has_key?(LavinMQ::PoisonPill::SourceStamp::HEADER_SOURCE_QUEUE).should be_false
  end

  it "round-trips other AMQP property fields" do
    props = AMQ::Protocol::Properties.new(
      content_type: "application/json",
      delivery_mode: 2_u8,
      priority: 3_u8,
      correlation_id: "c1",
      message_id: "m1",
      app_id: "app",
    )
    stamped = LavinMQ::PoisonPill::SourceStamp.stamp(props, "q", "ex", "rk")
    stamped.content_type.should eq "application/json"
    stamped.delivery_mode.should eq 2_u8
    stamped.priority.should eq 3_u8
    stamped.correlation_id.should eq "c1"
    stamped.message_id.should eq "m1"
    stamped.app_id.should eq "app"
  end
end
