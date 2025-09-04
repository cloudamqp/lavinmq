require "./spec_helper"
require "./../src/lavinmq/amqp/stream/stream_reader"
require "./../src/lavinmq/amqp/stream/stream_message_store"

class MockStore
  include LavinMQ::AMQP::StreamStore

  @segments : Hash(Int32, Array(LavinMQ::Envelope))

  def initialize(@seg_size : Int32, msg_count)
    @segments = build_db(@seg_size, msg_count)
  end

  def find_offset(offset)
    {0u32, 0u32, 0u32}
  end

  def read(segment, position) : LavinMQ::Envelope?
    if seg = @segments[segment]?
      return seg[position]?
    end
    nil
  end

  private def build_db(seg_size, msg_count)
    db = Hash(Int32, Array(LavinMQ::Envelope)).new
    body = "hello".to_slice
    seg = 0
    msg_count.times do |i|
      seg = (i / seg_size).to_i if i % seg_size == 0
      sp = LavinMQ::SegmentPosition.new(seg.to_u32, 0u32, 1u32)
      props = AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"x-stream-offset": i}))

      msg = LavinMQ::BytesMessage.new(0i64, "", "", props, 5u64, body)
      db[seg] ||= Array(LavinMQ::Envelope).new
      db[seg] << LavinMQ::Envelope.new(sp, msg)
    end
    db
  end
end

describe LavinMQ::AMQP::StreamReader do
  it "should get message from msg store" do
    s = MockStore.new 5, 5
    r = LavinMQ::AMQP::StreamReader.new(s, 0)
    count = 5
    i = 0
    r.each do |e|
      e.should_not be_nil
      count -= 1
      i += 1
      break if count.zero?
    end
    i.should eq 5
  end
end
