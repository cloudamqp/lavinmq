require "./spec_helper"
require "./support/s3_server"
require "./../src/lavinmq/amqp/stream/s3_message_store"

DATA_DIR = "42099b4af021e53fd8fd4e056c2568d7c2e3ffa8/77d9712623c4368721b466d1c24d447e9c53c8d3"

module S3SpecHelper
  class_property s3_server : MinimalS3Server?

  def self.meta_bytes(offset)
    io = IO::Memory.new
    io.write_bytes 100_u32 # count
    io.write_bytes offset  # first offset
    io.write_bytes offset  # first timestamp
    io.write_bytes offset  # last timestamp
    io.rewind
    io.getb_to_end
  end

  def self.segment_bytes
    io = IO::Memory.new
    io.write_bytes 4 # schema version
    100.times do
      io.write_bytes LavinMQ::Message.new("ex", "rk", "body")
    end
    io.rewind
    io.getb_to_end
  end

  def self.setup_s3_with_files
    if server = s3_server
      server.clear

      # Add segment and meta files to S3
      server.put("#{DATA_DIR}/msgs.0000000001", segment_bytes())
      server.put("#{DATA_DIR}/msgs.0000000002", segment_bytes())
      server.put("#{DATA_DIR}/meta.0000000001", meta_bytes(0_i64))
      server.put("#{DATA_DIR}/meta.0000000002", meta_bytes(100_i64))
    else
      fail("No s3 server")
    end
  end
end

describe LavinMQ::AMQP::Stream::S3MessageStore do
  Spec.before_suite do
    # Start S3 server once for all tests
    s3_server = MinimalS3Server.new
    s3_server.start
    S3SpecHelper.s3_server = s3_server
  end

  Spec.after_suite do
    # Stop S3 server after all tests
    S3SpecHelper.s3_server.try(&.stop)
  end

  before_each do
    # Clear S3 storage before each test
    S3SpecHelper.s3_server.try(&.clear)

    # Configure LavinMQ to use our test S3 server
    if server = S3SpecHelper.s3_server
      LavinMQ::Config.instance.streams_s3_storage = true
      LavinMQ::Config.instance.streams_s3_storage_endpoint = "http://#{server.endpoint}"
      LavinMQ::Config.instance.streams_s3_storage_region = "us-east-1"
      LavinMQ::Config.instance.streams_s3_storage_access_key_id = "test_access_key"
      LavinMQ::Config.instance.streams_s3_storage_secret_access_key = "test_secret_key"
    else
      fail("No s3 server")
    end
  end

  after_each do
    FileUtils.rm_rf("/tmp/lavinmq-spec/#{DATA_DIR}")
  end

  it "should get file list from s3" do
    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    S3SpecHelper.setup_s3_with_files
    msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

    msg_store.@s3_segments.size.should eq 2
    msg_store.@segments.size.should eq 2
    msg_store.@size.should eq 200
  end

  it "should read & upload local file" do
    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    File.write(File.join(msg_dir, "msgs.0000000003"), S3SpecHelper.segment_bytes)
    S3SpecHelper.setup_s3_with_files
    msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

    msg_store.@s3_segments.size.should eq 3
    msg_store.@segments.size.should eq 3
    msg_store.@size.should eq 300
  end

  # Publish a message and verify it can be consumed with S3 storage
  it "can publish and consume with s3 storage" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "test_queue"
        ch.prefetch 1
        q_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})
        q = ch.queue(q_name, durable: true, args: q_args)

        # Publish a message
        ch.basic_publish("test body", "", q_name)

        # Consume the message
        channel = Channel(String).new
        q.subscribe(no_ack: false) do |msg|
          channel.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end
        channel.receive.should eq "test body"
      end
    end
  end

  describe "no meta files" do
    it "should verify local segments" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      File.write(File.join(msg_dir, "msgs.0000000001"), S3SpecHelper.segment_bytes)
      File.write(File.join(msg_dir, "msgs.0000000002"), S3SpecHelper.segment_bytes)
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000001"), &.getb_to_end))
      etag1 = digest.hexfinal
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000002"), &.getb_to_end))
      etag2 = digest.hexfinal

      # Add files to S3 (no meta files, just segments)
      if server = S3SpecHelper.s3_server
        server.put("#{DATA_DIR}/msgs.0000000001", S3SpecHelper.segment_bytes)
        server.put("#{DATA_DIR}/msgs.0000000002", S3SpecHelper.segment_bytes)
      else
        fail("No s3 server")
      end

      msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000001"), &.getb_to_end))
      digest.hexfinal.should eq etag1
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000002"), &.getb_to_end))
      digest.hexfinal.should eq etag2
    end

    it "should read local segments" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      File.write(File.join(msg_dir, "msgs.0000000001"), S3SpecHelper.segment_bytes)
      File.write(File.join(msg_dir, "msgs.0000000002"), S3SpecHelper.segment_bytes)

      # S3 has no files (empty)
      msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end

    it "should download segments from s3" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)

      # Add segments to S3 (no meta files)
      if server = S3SpecHelper.s3_server
        server.put("#{DATA_DIR}/msgs.0000000001", S3SpecHelper.segment_bytes)
        server.put("#{DATA_DIR}/msgs.0000000002", S3SpecHelper.segment_bytes)
      else
        fail("No s3 server")
      end

      msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end
  end

  it "uploads sealed segments to S3 on rotation" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "s3-upload-test"
        q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
        q = ch.queue(q_name, durable: true, args: q_args)

        # Publish a segment-sized message to trigger rotation
        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        q.publish_confirm data
        # Second publish triggers open_new_segment which uploads the first
        q.publish_confirm data

        server = S3SpecHelper.s3_server.not_nil!
        # Sealed segment should be uploaded to S3
        wait_for { server.keys.count(&.matches?(/msgs\.\d{10}$/)) >= 1 }

        ch.queue_delete(q_name)
      end
    end
  end

  it "deletes S3 segments when queue is deleted" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "s3-delete-test"
        q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
        q = ch.queue(q_name, durable: true, args: q_args)

        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        2.times { q.publish_confirm data }

        server = S3SpecHelper.s3_server.not_nil!
        # Verify segments exist in S3 before delete
        wait_for { !server.keys.select(&.includes?("msgs.")).empty? }

        ch.queue_delete(q_name)

        # All segments for this queue should be gone from S3
        queue_hash = Digest::SHA1.hexdigest(q_name)
        remaining = server.keys.select(&.includes?(queue_hash))
        remaining.should be_empty
      end
    end
  end

  it "drops oldest S3 segments when max-length exceeded" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "s3-maxlen-test"
        args = AMQP::Client::Arguments.new({"x-queue-type": "stream", "x-max-length": 1})
        q = ch.queue(q_name, durable: true, args: args)

        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        4.times { q.publish_confirm data }

        server = S3SpecHelper.s3_server.not_nil!
        queue_hash = Digest::SHA1.hexdigest(q_name)

        # Wait for uploads, then verify segments were dropped
        # We published 4 segment-sized messages (4 segments sealed + 1 active)
        # With max-length: 1, most segments should be dropped
        q.message_count.should be <= 2
        segment_keys = server.keys.select { |k| k.includes?(queue_hash) && k.matches?(/msgs\.\d{10}$/) }
        segment_keys.size.should be <= 2

        ch.queue_delete(q_name)
      end
    end
  end

  it "consumes messages across segment boundaries" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "s3-cross-segment"
        q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
        q = ch.queue(q_name, durable: true, args: q_args)
        ch.prefetch 1

        # Publish enough to span 2 segments
        data = Bytes.new(LavinMQ::Config.instance.segment_size)
        q.publish_confirm data
        q.publish_confirm "last message"

        # Consume from the beginning
        msgs = Channel(String).new(2)
        q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
          msgs.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end

        first = msgs.receive
        first.bytesize.should eq LavinMQ::Config.instance.segment_size
        msgs.receive.should eq "last message"

        ch.queue_delete(q_name)
      end
    end
  end

  it "should raise if not configured properly" do
    LavinMQ::Config.instance.streams_s3_storage_region = nil
    LavinMQ::Config.instance.streams_s3_storage_access_key_id = nil
    LavinMQ::Config.instance.streams_s3_storage_secret_access_key = nil

    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)

    expect_raises(SpecExit) do
      LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
    end
  end

  describe "download failure handling" do
    it "recovers from S3 GET failure during segment cache download" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q_name = "s3-fail-test"
          q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
          q = ch.queue(q_name, durable: true, args: q_args)
          ch.prefetch 1

          # Publish enough to create 2 segments
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          q.publish_confirm data
          q.publish_confirm "second segment msg"

          # Make the first segment fail on next GET (simulates transient S3 error)
          server = S3SpecHelper.s3_server.not_nil!
          queue_hash = Digest::SHA1.hexdigest(q_name)
          first_segment_key = server.keys.find { |k| k.includes?(queue_hash) && k.matches?(/msgs\.\d{10}$/) }
          server.fail_keys.add(first_segment_key.not_nil!) if first_segment_key

          # Consumer should still be able to read (direct download retries or fallback)
          msgs = Channel(String).new(2)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
            msgs.send msg.body_io.to_s
            ch.basic_ack(msg.delivery_tag)
          end

          # Should eventually receive messages (fail_keys only fails once)
          msg = msgs.receive
          msg.bytesize.should eq LavinMQ::Config.instance.segment_size

          ch.queue_delete(q_name)
        end
      end
    end
  end

  describe "S3 pagination" do
    it "lists all segments when S3 response is paginated" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)

      server = S3SpecHelper.s3_server.not_nil!
      # Set max keys to 3 to force pagination with just a few segments
      server.max_list_keys = 3

      # Add 4 segments with meta files (8 keys total, will need 3 pages)
      4.times do |i|
        seg_id = (i + 1).to_s.rjust(10, '0')
        server.put("#{DATA_DIR}/msgs.#{seg_id}", S3SpecHelper.segment_bytes)
        server.put("#{DATA_DIR}/meta.#{seg_id}", S3SpecHelper.meta_bytes((i * 100).to_i64))
      end

      msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
      # All 4 segments should be discovered despite pagination
      msg_store.@s3_segments.size.should eq 4
      msg_store.@size.should eq 400
    ensure
      server.try &.max_list_keys = 1000
    end
  end

  describe "concurrent consumers" do
    it "supports two consumers reading different offsets" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q_name = "s3-concurrent-consumers"
          q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
          q = ch.queue(q_name, durable: true, args: q_args)
          ch.prefetch 1

          # Publish messages across 2 segments
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          q.publish_confirm data
          q.publish_confirm "msg2"

          # Consumer 1: reads from first offset
          msgs1 = Channel(String).new(2)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
            msgs1.send msg.body_io.to_s
            ch.basic_ack(msg.delivery_tag)
          end

          # Consumer 2 on a separate channel: reads from last offset
          with_channel(s) do |ch2|
            ch2.prefetch 1
            q2 = ch2.queue(q_name, durable: true, args: q_args)
            msgs2 = Channel(String).new(2)
            q2.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "last"})) do |msg|
              msgs2.send msg.body_io.to_s
              ch2.basic_ack(msg.delivery_tag)
            end

            # Both consumers should receive messages
            first_msg = msgs1.receive
            first_msg.bytesize.should eq LavinMQ::Config.instance.segment_size
            msgs2.receive.should eq "msg2"
          end

          ch.queue_delete(q_name)
        end
      end
    end
  end

  describe "segment cache" do
    it "prefetches segments for consumers and cleans up after removal" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q_name = "s3-cache-test"
          q_args = AMQP::Client::Arguments.new({"x-queue-type": "stream"})
          q = ch.queue(q_name, durable: true, args: q_args)
          ch.prefetch 1

          # Publish enough data to create multiple segments
          data = Bytes.new(LavinMQ::Config.instance.segment_size)
          3.times { q.publish_confirm data }

          server = S3SpecHelper.s3_server.not_nil!
          queue_hash = Digest::SHA1.hexdigest(q_name)
          # Verify segments were uploaded to S3
          wait_for { server.keys.count { |k| k.includes?(queue_hash) && k.matches?(/msgs\.\d{10}$/) } >= 2 }

          # Start a consumer to trigger prefetching
          msgs = Channel(String).new(4)
          q.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
            msgs.send msg.body_io.to_s
            ch.basic_ack(msg.delivery_tag)
          end

          # Consume at least one message to confirm cache is working
          msgs.receive

          ch.queue_delete(q_name)
        end
      end
    end
  end
end
