require "./spec_helper"
require "./../src/lavinmq/amqp/queue/stream_s3_message_store"
DATA_DIR = "42099b4af021e53fd8fd4e056c2568d7c2e3ffa8/77d9712623c4368721b466d1c24d447e9c53c8d3"

module S3SpecHelper
  class_property responses : Hash(String, String | Bytes) = S3SpecHelper.setup_responses

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

  def self.setup_responses : Hash(String, String | Bytes)
    {
      "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => RESPONSE_FILE_LIST,
      "/#{DATA_DIR}/msgs.0000000001"                         => S3SpecHelper.segment_bytes(0_i64),
      "/#{DATA_DIR}/msgs.0000000002"                         => S3SpecHelper.segment_bytes(100_i64),
      "/#{DATA_DIR}/msgs.0000000001.meta"                    => S3SpecHelper.meta_bytes(0_i64),
      "/#{DATA_DIR}/msgs.0000000002.meta"                    => S3SpecHelper.meta_bytes(100_i64),
      "/tmp/lavinmq-spec#{DATA_DIR}"                         => RESPONSE_UPLOAD,
    }
  end
end

RESPONSE_FILE_LIST = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001</Key>
              <ETag>"abc123"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002</Key>
              <ETag>"def456"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002.meta</Key>
              <ETag>"meta456"</ETag>
              <Size>20</Size>
            </Contents>
          </ListBucketResult>
          XML
RESPONSE_UPLOAD = ""

class MockS3HTTPClient < HTTP::Client
  def initialize(hostname)
    super(hostname, 443, tls: true)
  end

  def get(path, headers = nil)
    match_response(path)
  end

  def put(path, body = nil, headers = nil)
    match_response(path, "PUT")
  end

  def delete(path, headers = nil)
    match_response(path, "DELETE")
  end

  def get(path, headers = nil, &)
    yield match_response(path)
  end

  def match_response(path, method = "GET") : HTTP::Client::Response
    resp = ""
    if path.starts_with?("?delimiter=%2F&encoding-type=url&list-type=2&prefix=")
      resp = S3SpecHelper.responses.first_value
    else
      S3SpecHelper.responses.each do |key, response|
        if path == key
          resp = response
        end
      end
    end
    if resp == "" && method == "GET"
      HTTP::Client::Response.new(404, "Not Found")
    elsif resp.is_a?(String)
      body_io = IO::Memory.new(resp)
      headers = HTTP::Headers{"Content-Length" => resp.bytesize.to_s, "ETag" => "abc123"}
      HTTP::Client::Response.new(200, resp, headers, body_io: body_io)
    else
      body_io = IO::Memory.new(resp)
      headers = HTTP::Headers{"Content-Length" => body_io.bytesize.to_s, "ETag" => "abc123"}
      HTTP::Client::Response.new(200, "", headers, body_io: body_io)
    end
  end
end

# Mock S3 storage client that uses the mock HTTP client
class LavinMQ::AMQP::S3StorageClient
  def http_client(with_timeouts = false)
    h = MockS3HTTPClient.new("test.lavinmq.com")

    if signer = s3_signer
      signer.sign(::HTTP::Request.new("GET", "test.lavinmq.com"))
    else
      raise "No S3 signer found"
    end
    if with_timeouts
      h.connect_timeout = 200.milliseconds
      h.read_timeout = 500.milliseconds
    end
    h.as(::HTTP::Client)
  end
end

describe LavinMQ::AMQP::StreamQueue::StreamS3MessageStore do
  before_each do
    LavinMQ::Config.instance.streams_s3_storage = true
    LavinMQ::Config.instance.streams_s3_storage_bucket = "test_bucket"
    LavinMQ::Config.instance.streams_s3_storage_endpoint = "test.lavinmq.com"
    LavinMQ::Config.instance.streams_s3_storage_region = "us-east-1"
    LavinMQ::Config.instance.streams_s3_storage_access_key_id = "test_access_key"
    LavinMQ::Config.instance.streams_s3_storage_secret_access_key = "test_secret_key"
  end

  after_each do
    FileUtils.rm_rf("/tmp/lavinmq-spec/#{DATA_DIR}")
  end

  it "should get file list from s3" do
    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    S3SpecHelper.responses = S3SpecHelper.setup_responses
    msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

    msg_store.@s3_segments.size.should eq 2
    msg_store.@segments.size.should eq 2
    msg_store.@size.should eq 200
  end

  it "should read & upload local file" do
    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    File.write(File.join(msg_dir, "msgs.0000000003"), S3SpecHelper.segment_bytes(200_i64))
    S3SpecHelper.responses = S3SpecHelper.setup_responses
    msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

    msg_store.@s3_segments.size.should eq 3
    msg_store.@segments.size.should eq 3
    msg_store.@size.should eq 300
  end

  # Starting with empty data dir, download files from s3 and consume
  it "can consume from s3" do
    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_name = "test_queue"
        ch.prefetch 1
        q_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})
        q = ch.queue(q_name, durable: true, args: q_args)
        channel = Channel(String).new
        q.subscribe(no_ack: false) do |msg|
          channel.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end
        channel.receive.should eq "body"
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

      file_list = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001</Key>
              <ETag>"#{etag1}"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002</Key>
              <ETag>"#{etag2}"</ETag>
              <Size>5647</Size>
            </Contents>
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/#{DATA_DIR}/msgs.0000000001"                         => "foo".to_slice,
        "/#{DATA_DIR}/msgs.0000000002"                         => "bar".to_slice,
        "/tmp/lavinmq-spec/#{DATA_DIR}"                        => RESPONSE_UPLOAD,
      }
      S3SpecHelper.responses = responses
      msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

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
      file_list = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/#{DATA_DIR}/msgs.0000000001"                         => S3SpecHelper.segment_bytes(0_i64),
        "/#{DATA_DIR}/msgs.0000000002"                         => S3SpecHelper.segment_bytes(100_i64),
        "/tmp/lavinmq-spec/#{DATA_DIR}"                        => RESPONSE_UPLOAD,
      }
      S3SpecHelper.responses = responses
      msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)

      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end

    it "should download segments from s3" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      file_list = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001</Key>
              <ETag>"abc"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/#{DATA_DIR}/msgs.0000000001"                         => S3SpecHelper.segment_bytes(0_i64),
        "/#{DATA_DIR}/msgs.0000000002"                         => S3SpecHelper.segment_bytes(100_i64),
        "/tmp/lavinmq-spec/#{DATA_DIR}"                        => RESPONSE_UPLOAD,
      }
      S3SpecHelper.responses = responses
      msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end

    it "should download segments in the right order" do
      msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      file_list = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001</Key>
              <ETag>"abc"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000003</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000004</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000005</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000001.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000002.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000003.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000004.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>#{DATA_DIR}/msgs.0000000005.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/#{DATA_DIR}/msgs.0000000001"                         => S3SpecHelper.segment_bytes(0_i64),
        "/#{DATA_DIR}/msgs.0000000002"                         => S3SpecHelper.segment_bytes(100_i64),
        "/#{DATA_DIR}/msgs.0000000003"                         => S3SpecHelper.segment_bytes(200_i64),
        "/#{DATA_DIR}/msgs.0000000004"                         => S3SpecHelper.segment_bytes(300_i64),
        "/#{DATA_DIR}/msgs.0000000005"                         => S3SpecHelper.segment_bytes(400_i64),
        "/#{DATA_DIR}/msgs.0000000001.meta"                    => S3SpecHelper.meta_bytes(0_i64),
        "/#{DATA_DIR}/msgs.0000000002.meta"                    => S3SpecHelper.meta_bytes(100_i64),
        "/#{DATA_DIR}/msgs.0000000003.meta"                    => S3SpecHelper.meta_bytes(200_i64),
        "/#{DATA_DIR}/msgs.0000000004.meta"                    => S3SpecHelper.meta_bytes(300_i64),
        "/#{DATA_DIR}/msgs.0000000005.meta"                    => S3SpecHelper.meta_bytes(400_i64),
        "/tmp/lavinmq-spec/#{DATA_DIR}"                        => RESPONSE_UPLOAD,
      }
      S3SpecHelper.responses = responses
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q_name = "test_queue"
          ch.prefetch 1
          q_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})
          q = ch.queue(q_name, durable: true, args: q_args)
          channel = Channel(String).new
          q.subscribe(no_ack: false) do |msg|
            channel.send msg.body_io.to_s
            ch.basic_ack(msg.delivery_tag)
          end
          500.times do
            channel.receive.should eq "body"
          end
        end
      end
    end
  end

  it "should raise if not configed properly" do
    LavinMQ::Config.instance.streams_s3_storage_region = nil
    LavinMQ::Config.instance.streams_s3_storage_access_key_id = nil
    LavinMQ::Config.instance.streams_s3_storage_secret_access_key = nil

    msg_dir = "/tmp/lavinmq-spec/#{DATA_DIR}"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    S3SpecHelper.responses = S3SpecHelper.setup_responses

    expect_raises(SpecExit) do
      LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil, true, ::Log::Metadata.empty)
    end
  end
end
