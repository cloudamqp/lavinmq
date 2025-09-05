require "./spec_helper"
require "./../src/lavinmq/amqp/queue/stream_reader"
require "./../src/lavinmq/amqp/queue/stream_queue_message_store"

class MockStore
  include LavinMQ::AMQP::StreamQueue::MsgStore

  def find_offset(offset)
    {0u32, 0u32, 0u32}
  end

  def read(segment, position)
    nil
  end
end

describe LavinMQ::AMQP::StreamReader do
  it "should get message with offset 2" do
    s = MockStore.new
    r = LavinMQ::AMQP::StreamReader.new(s, 0)
    r.each do |e|
      puts e
    end
  end
end
