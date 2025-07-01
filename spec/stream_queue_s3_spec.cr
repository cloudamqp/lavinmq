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
    99.times do |i|
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-stream-offset" => offset + i,
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      io.write_bytes msg
    end
    io.rewind
    io.getb_to_end
  end
end

RESPONSE_FILE_LIST = <<-XML
          <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult>
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
  @responses = Hash(String, String | Bytes).new

  def initialize(hostname)
    super(hostname, 443, tls: true)
    setup_responses
  end

  def setup_responses
    @responses = {
      "?delimiter=%2F&encoding-type=url&list-type=2&prefix=" => RESPONSE_FILE_LIST,
      "/test_queue/msgs.0000000001"                          => S3SpecHelper.segment_bytes(0_i64),
      "/test_queue/msgs.0000000002"                          => S3SpecHelper.segment_bytes(100_i64),
      "/test_queue/msgs.0000000001.meta"                     => S3SpecHelper.meta_bytes(0_i64),
      "/test_queue/msgs.0000000002.meta"                     => S3SpecHelper.meta_bytes(100_i64),
      "/tmp/lavinmq-spec/test_queue"                         => RESPONSE_UPLOAD,
    }
  end

  def get(path, headers = nil)
    match_response(path)
  end

  def put(path, body = nil, headers = nil)
    body_str = body.is_a?(Bytes) ? String.new(body) : body.to_s
    match_response(path)
  end

  def delete(path, headers = nil)
    match_response(path)
  end

  def get(path, headers = nil, &block : HTTP::Client::Response -> T) : T forall T
    yield match_response(path)
  end

  def match_response(path) : HTTP::Client::Response
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
    if resp.is_a?(String)
      body_io = IO::Memory.new(resp)
      headers = HTTP::Headers{"Content-Length" => resp.bytesize.to_s, "ETag" => "abc123"}
      return HTTP::Client::Response.new(200, resp, headers, body_io: body_io)
    else
      body_io = IO::Memory.new(resp)
      headers = HTTP::Headers{"Content-Length" => body_io.bytesize.to_s, "ETag" => "abc123"}
      return HTTP::Client::Response.new(200, "", headers, body_io: body_io)
    end
    pp path
    HTTP::Client::Response.new(404, "Not Found")
  end
end

class LavinMQ::AMQP::StreamQueue::StreamS3MessageStore
  def http(uri : URI)
    h = MockS3HTTPClient.new("test.lavinmq.com")
    h.before_request do |request|
      if signer = s3_signer
        signer.sign(request)
      else
        raise "No S3 signer found"
      end
    end
    h.as(::HTTP::Client)
  end
end

# do config?

describe LavinMQ::AMQP::StreamQueue::StreamS3MessageStore do
  it "should get file list from s3" do
    with_datadir do |data_dir|
      msg_dir = "/tmp/lavinmq-spec/test_queue"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil)

      msg_store.@s3_segments.size.should eq 2
      msg_store.@segments.size.should eq 1
      pp msg_store.@segment_msg_count
    end
  end

  it "should read & upload local file" do
    with_datadir do |data_dir|
      msg_dir = "/tmp/lavinmq-spec/test_queue"
      FileUtils.rm_rf(msg_dir)
      Dir.mkdir_p(msg_dir)
      File.write(File.join(msg_dir, "msgs.0000000003"), S3SpecHelper.segment_bytes(200_i64))
      msg_store = LavinMQ::AMQP::StreamQueue::StreamS3MessageStore.new(msg_dir, nil)

      msg_store.@s3_segments.size.should eq 3
      msg_store.@segments.size.should eq 3
      pp msg_store.@segment_msg_count
    end
  end
end
