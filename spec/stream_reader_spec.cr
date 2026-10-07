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

        iq = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Stream)
        stream = iq.reader LavinMQ::AMQP::StreamOffset::Absolute.new(5)

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
  it "should include x-stream-offset header" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("streams", "direct")
        q = ch.queue("", args: AMQP::Client::Arguments.new({
          "x-queue-type" => "stream",
        }))
        q.bind(x.name, q.name)
        3.times do |i|
          x.publish_confirm("test message #{i}", q.name)
        end

        iq = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Stream)
        stream = iq.reader LavinMQ::AMQP::StreamOffset::First.new

        count = 0
        stream.each do |env|
          headers = env.message.properties.headers
          headers.should_not be_nil
          headers.not_nil!["x-stream-offset"].should eq (count + 1).to_i64
          count += 1
        end
        count.should eq 3
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
        ch.confirm_select
        400.times do |i|
          x.publish("test message #{i}" * 100, q.name)
        end
        ch.wait_for_confirms

        iq = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Stream)
        stream = iq.reader LavinMQ::AMQP::StreamOffset::Absolute.new(0)

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

  it "keeps reading when retention drops the segment being read" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        segment_size = LavinMQ::Config.instance.segment_size
        body = "x" * (segment_size // 4)
        q = ch.queue("", args: AMQP::Client::Arguments.new({
          "x-queue-type" => "stream", "x-max-length-bytes" => segment_size.to_i64 * 2,
        }))
        12.times { q.publish_confirm body }

        iq = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Stream)
        store = iq.stream_msg_store
        first_seg = store.@segments.first_key
        offsets = [] of Int64
        dropped = false
        iq.reader(LavinMQ::AMQP::StreamOffset::First.new).each do |env|
          offsets << env.message.properties.headers.not_nil!["x-stream-offset"].as(Int64)
          unless dropped
            20.times do
              break unless store.@segments.has_key?(first_seg)
              iq.publish(LavinMQ::Message.new("", q.name, body))
            end
            dropped = true
          end
          String.new(env.message.body).should eq body
        end
        store.@segments.has_key?(first_seg).should be_false
        # Skips the dropped messages, with offsets matching the messages read
        offsets.first.should eq 1
        offsets[1].should be > 2
        offsets.skip(1).each_cons_pair { |a, b| b.should eq a + 1 }
        offsets.last.should eq iq.last_offset
      end
    end
  end
end
