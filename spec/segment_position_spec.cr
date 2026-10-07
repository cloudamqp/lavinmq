require "spec"
require "../src/lavinmq/amqp"
require "../src/lavinmq/message"
require "../src/lavinmq/segment_position"

describe LavinMQ::SegmentPosition do
  subject = LavinMQ::SegmentPosition
  describe "with expiration" do
    it "should create a SP with x-delay" do
      headers = LavinMQ::AMQP::Table.new({"x-delay" => 15})
      props = LavinMQ::AMQP::Properties.new(headers: headers)
      msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
      sp = subject.make(1u32, 1u32, msg)
      sp.delay.should eq 15
      sp.priority.should eq 0
    end
  end

  it "should create a SP with priority" do
    segment = 0_u32
    position = 0_u32
    props = LavinMQ::AMQP::Properties.new(priority: 6_u8)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.delay.should eq 0
    sp.priority.should eq 6
  end

  it "should create a SP without TTL or priority" do
    segment = 0_u32
    position = 0_u32
    props = LavinMQ::AMQP::Properties.new
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(segment, position, msg)
    sp.delay.should eq 0
    sp.priority.should eq 0
  end

  it "should flag SP with x-dead-letter-exchange header" do
    headers = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
    props = LavinMQ::AMQP::Properties.new(headers: headers)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(1u32, 1u32, msg)
    sp.has_dlx?.should be_true
    sp.delay.should eq 0
  end

  it "should find x-delay and x-dead-letter-exchange among other headers" do
    headers = LavinMQ::AMQP::Table.new({
      "app" => "spec", "x-dead-letter-exchange" => "dlx", "x-delay" => 25, "other" => 1,
    })
    props = LavinMQ::AMQP::Properties.new(headers: headers)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(1u32, 1u32, msg)
    sp.has_dlx?.should be_true
    sp.delay.should eq 25
  end

  it "should treat negative x-delay as no delay" do
    headers = LavinMQ::AMQP::Table.new({"x-delay" => -5})
    props = LavinMQ::AMQP::Properties.new(headers: headers)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(1u32, 1u32, msg)
    sp.delay.should eq 0
  end

  it "should treat non-integer x-delay as no delay" do
    headers = LavinMQ::AMQP::Table.new({"x-delay" => "soon"})
    props = LavinMQ::AMQP::Properties.new(headers: headers)
    msg = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body"))
    sp = subject.make(1u32, 1u32, msg)
    sp.delay.should eq 0
  end
end
