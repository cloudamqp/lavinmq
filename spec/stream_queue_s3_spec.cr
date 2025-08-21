require "./spec_helper"
require "./../src/lavinmq/amqp/queue/stream_s3_message_store"

class S3SpecHelper
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
      "/test_queue/msgs.0000000001"                          => S3SpecHelper.segment_bytes(0_i64),
      "/test_queue/msgs.0000000002"                          => S3SpecHelper.segment_bytes(100_i64),
      "/test_queue/msgs.0000000001.meta"                     => S3SpecHelper.meta_bytes(0_i64),
      "/test_queue/msgs.0000000002.meta"                     => S3SpecHelper.meta_bytes(100_i64),
      "/tmp/lavinmq-spec/test_queue"                         => RESPONSE_UPLOAD,
    }
  end
end

RESPONSE_FILE_LIST = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>test_queue/msgs.0000000001</Key>
              <ETag>"abc123"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>test_queue/msgs.0000000002</Key>
              <ETag>"def456"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>test_queue/msgs.0000000001.meta</Key>
              <ETag>"meta123"</ETag>
              <Size>20</Size>
            </Contents>
            <Contents>
              <Key>test_queue/msgs.0000000002.meta</Key>
              <ETag>"meta456"</ETag>
              <Size>20</Size>
            </Contents>
          </ListBucketResult>
          XML
RESPONSE_UPLOAD = ""

class MockS3HTTPClient < HTTP::Client
  property responses = Hash(String, String | Bytes).new

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
      resp = @responses.first_value
    else
      @responses.each do |key, response|
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

# Mock HTTP client for S3 specs
class MockStreamS3MessageStore < LavinMQ::AMQP::StreamQueue::StreamS3MessageStore
  @test_responses = Hash(String, String | Bytes).new

  def initialize(@msg_dir : String, @test_responses : Hash(String, String | Bytes) = S3SpecHelper.setup_responses)
    super(@msg_dir, nil, true, ::Log::Metadata.empty)
  end

  def http_client(with_timeouts = false)
    h = MockS3HTTPClient.new("test.lavinmq.com")
    h.responses = @test_responses

    if signer = s3_signer
      signer.sign(::HTTP::Request.new("GET", "test.lavinmq.com"))
    else
      raise "No S3 signer found"
    end
    if with_timeouts
      h.connect_timeout = HTTP_CONNECT_TIMEOUT
      h.read_timeout = HTTP_READ_TIMEOUT
    end
    h.as(::HTTP::Client)
  end
end

describe LavinMQ::AMQP::StreamQueue::StreamS3MessageStore do
  before_each do
    LavinMQ::Config.instance.streams_s3_storage_region = "us-east-1"
    LavinMQ::Config.instance.streams_s3_storage_access_key_id = "test_access_key"
    LavinMQ::Config.instance.streams_s3_storage_secret_access_key = "test_secret_key"
  end

  after_each do
    FileUtils.rm_rf("/tmp/lavinmq-spec/test_queue")
  end

  it "should get file list from s3" do
    msg_dir = "/tmp/lavinmq-spec/test_queue"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    msg_store = MockStreamS3MessageStore.new(msg_dir, S3SpecHelper.setup_responses)

    msg_store.@s3_segments.size.should eq 2
    msg_store.@segments.size.should eq 2
    msg_store.@size.should eq 200
  end

  it "should read & upload local file" do
    msg_dir = "/tmp/lavinmq-spec/test_queue"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    File.write(File.join(msg_dir, "msgs.0000000003"), S3SpecHelper.segment_bytes(200_i64))
    msg_store = MockStreamS3MessageStore.new(msg_dir, S3SpecHelper.setup_responses)

    msg_store.@s3_segments.size.should eq 3
    msg_store.@segments.size.should eq 3
    msg_store.@size.should eq 300
  end

  # Starting with empty data dir, download files from s3 and consume
  it "can consume from s3" do
    msg_dir = "/tmp/lavinmq-spec/test_queue"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)
    msg_store = MockStreamS3MessageStore.new(msg_dir, S3SpecHelper.setup_responses)

    consumer = MockStreamConsumer.new(0_i64, 0_u32, 0)
    if env = msg_store.shift?(consumer)
      String.new(env.message.body).should eq "body"
    else
      fail "Expected to get a message from S3"
    end
  end

  describe "no meta files" do
    it "should verify local segments" do
      msg_dir = "/tmp/lavinmq-spec/test_queue"
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
              <Key>test_queue/msgs.0000000001</Key>
              <ETag>"#{etag1}"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>test_queue/msgs.0000000002</Key>
              <ETag>"#{etag2}"</ETag>
              <Size>5647</Size>
            </Contents>
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/test_queue/msgs.0000000001"                          => "foo".to_slice,
        "/test_queue/msgs.0000000002"                          => "bar".to_slice,
        "/tmp/lavinmq-spec/test_queue"                         => RESPONSE_UPLOAD,
      }
      msg_store = MockStreamS3MessageStore.new(msg_dir, responses)

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
      msg_dir = "/tmp/lavinmq-spec/test_queue"
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
        "/test_queue/msgs.0000000001"                          => S3SpecHelper.segment_bytes(0_i64),
        "/test_queue/msgs.0000000002"                          => S3SpecHelper.segment_bytes(100_i64),
        "/tmp/lavinmq-spec/test_queue"                         => RESPONSE_UPLOAD,
      }
      msg_store = MockStreamS3MessageStore.new(msg_dir, responses)

      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end

    it "should download segments from s3" do
      msg_dir = "/tmp/lavinmq-spec/test_queue"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      file_list = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Contents>
              <Key>test_queue/msgs.0000000001</Key>
              <ETag>"abc"</ETag>
              <Size>5647</Size>
            </Contents>
            <Contents>
              <Key>test_queue/msgs.0000000002</Key>
              <ETag>"cde"</ETag>
              <Size>5647</Size>
            </Contents>
          </ListBucketResult>
          XML
      responses = {
        "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => file_list,
        "/test_queue/msgs.0000000001"                          => S3SpecHelper.segment_bytes(0_i64),
        "/test_queue/msgs.0000000002"                          => S3SpecHelper.segment_bytes(100_i64),
        "/tmp/lavinmq-spec/test_queue"                         => RESPONSE_UPLOAD,
      }
      msg_store = MockStreamS3MessageStore.new(msg_dir, responses)
      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 3
      msg_store.@size.should eq 200
    end
  end

  it "should raise if not configed properly" do
    LavinMQ::Config.instance.streams_s3_storage_region = nil
    LavinMQ::Config.instance.streams_s3_storage_access_key_id = nil
    LavinMQ::Config.instance.streams_s3_storage_secret_access_key = nil

    msg_dir = "/tmp/lavinmq-spec/test_queue"
    FileUtils.rm_rf(msg_dir)
    Dir.mkdir_p(msg_dir)

    expect_raises(SpecExit) do
      MockStreamS3MessageStore.new(msg_dir, S3SpecHelper.setup_responses)
    end
  end
end

class MockStreamConsumer < LavinMQ::AMQP::StreamConsumer
  @channel = MockChannel.new
  @tag = "test_tag"

  def initialize(@offset : Int64, @segment : UInt32 = 0, @pos : UInt32 = 0)
    @users = LavinMQ::Auth::UserStore.new("/tmp/lavinmq-spec/test_queue", LavinMQ::Clustering::NoopServer.new)
    @vhost = LavinMQ::VHost.new("vhost", "/tmp/lavinmq-spec/test_queue", @users, LavinMQ::Clustering::NoopServer.new)
    @queue = LavinMQ::AMQP::Queue.new(@vhost, "test_queue", arguments: LavinMQ::AMQP::Table.new)
  end
end

class MockChannel < LavinMQ::AMQP::Channel
  @client = MockClient.new
  @id = 1
  @vhost = "/"
  @metadata = ::Log::Metadata.empty

  def initialize
    @log = LavinMQ::Logger.new(Log, @metadata)
  end
end

class MockClient < LavinMQ::AMQP::Client
  @socket = IO::Memory.new
  @connection_info = LavinMQ::ConnectionInfo.new(Socket::IPAddress.new("127.0.0.1", 5672), Socket::IPAddress.new("127.0.0.1", 5672))
  @user = LavinMQ::Auth::User.new("test_user", "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+", "SHA256", Array(LavinMQ::Tag).new)
  @max_frame_size = 4096_u32
  @actual_channel_max = 1000_u16
  @channel_max = 1000_u16
  @heartbeat_timeout = 60_u32
  @heartbeat_interval = 60_u32
  @auth_mechanism = "PLAIN"
  @client_properties = LavinMQ::AMQP::Table.new
  @metadata = ::Log::Metadata.empty

  def initialize
    users = LavinMQ::Auth::UserStore.new("/tmp/lavinmq-spec/test_queue", LavinMQ::Clustering::NoopServer.new)
    @vhost = LavinMQ::VHost.new("vhost", "/tmp/lavinmq-spec/test_queue", users, LavinMQ::Clustering::NoopServer.new)
    @log = LavinMQ::Logger.new(Log, @metadata)
  end
end
