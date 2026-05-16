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
        stream = iq.reader "first"

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
  it "envelope body survives concurrent segment drop" do
    # Regression: shift? / read returned BytesMessage whose body pointed
    # directly into the segment's mmap. drop_segments_while munmaps the
    # segment, and a slow consumer that's still copying bytes into the
    # socket would SIGSEGV in pointer copy_from. The fix detaches the body
    # off mmap before returning the envelope.
    with_amqp_server do |s|
      queue_name = ""
      with_channel(s) do |ch|
        q = ch.queue("", args: AMQP::Client::Arguments.new({
          "x-queue-type" => "stream",
        }))
        queue_name = q.name
        # Each message fills a segment so the next publish rolls a new one.
        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        data[0] = 0xAA_u8
        data[-1] = 0xBB_u8
        3.times { q.publish_confirm data }
      end

      stream = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Stream)
      reader = stream.reader "first"

      saved_env : LavinMQ::Envelope? = nil
      reader.each do |env|
        saved_env = env
        break
      end

      # Drop every segment but the last while we still hold an env from the
      # first segment — without the fix, mmap is gone and the next body
      # access SIGSEGVs.
      stream.@msg_store_lock.synchronize do
        store = stream.stream_msg_store
        store.max_length_bytes = 1_i64
        store.drop_overflow
      end

      saved_env.should_not be_nil
      env = saved_env.not_nil!
      env.message.bodysize.should eq LavinMQ::Config.instance.segment_size
      env.message.body.size.should eq LavinMQ::Config.instance.segment_size
      env.message.body[0].should eq 0xAA_u8
      env.message.body[-1].should eq 0xBB_u8
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

        iq = s.vhosts["/"].queue(q.name).as(LavinMQ::AMQP::Stream)
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
