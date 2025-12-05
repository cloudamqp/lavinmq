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
    io.rewind
    io.getb_to_end
  end

  def self.segment_bytes(offset)
    io = IO::Memory.new
    io.write_bytes 4 # schema version
    100.times do |i|
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-stream-offset" => offset + i,
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      io.write_bytes msg
    end
    io.rewind
    io.getb_to_end
  end

  def self.setup_s3_with_files
    if server = s3_server
      server.clear

      # Add segment and meta files to S3
      server.put("#{DATA_DIR}/msgs.0000000001", segment_bytes(0_i64))
      server.put("#{DATA_DIR}/msgs.0000000002", segment_bytes(100_i64))
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
    File.write(File.join(msg_dir, "msgs.0000000003"), S3SpecHelper.segment_bytes(200_i64))
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
      File.write(File.join(msg_dir, "msgs.0000000001"), S3SpecHelper.segment_bytes(0_i64))
      File.write(File.join(msg_dir, "msgs.0000000002"), S3SpecHelper.segment_bytes(100_i64))
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000001"), &.getb_to_end))
      etag1 = digest.hexfinal
      digest = Digest::MD5.new
      digest.update(File.open(File.join(msg_dir, "msgs.0000000002"), &.getb_to_end))
      etag2 = digest.hexfinal

      # Add files to S3 (no meta files, just segments)
      if server = S3SpecHelper.s3_server
        server.put("#{DATA_DIR}/msgs.0000000001", S3SpecHelper.segment_bytes(0_i64))
        server.put("#{DATA_DIR}/msgs.0000000002", S3SpecHelper.segment_bytes(100_i64))
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
      File.write(File.join(msg_dir, "msgs.0000000001"), S3SpecHelper.segment_bytes(0_i64))
      File.write(File.join(msg_dir, "msgs.0000000002"), S3SpecHelper.segment_bytes(100_i64))

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
        server.put("#{DATA_DIR}/msgs.0000000001", S3SpecHelper.segment_bytes(0_i64))
        server.put("#{DATA_DIR}/msgs.0000000002", S3SpecHelper.segment_bytes(100_i64))
      else
        fail("No s3 server")
      end

      msg_store = LavinMQ::AMQP::Stream::S3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end

    it "should download segments in the right order" do
    end
  end

  it "should raise if not configed properly" do
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
end
