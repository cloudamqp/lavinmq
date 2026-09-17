require "../spec_helper"

# Minimal client speaking the AWS JSON 1.0 protocol the way the SDKs do:
# POST with X-Amz-Target and a SigV4 Authorization header whose access key
# id names the LavinMQ user.
struct SQSSpecClient
  getter addr : Socket::IPAddress

  def initialize(@addr : Socket::IPAddress)
  end

  def base_url : String
    "http://#{@addr}"
  end

  def headers(user : String, region = "us-east-1") : HTTP::Headers
    HTTP::Headers{
      "Content-Type"  => "application/x-amz-json-1.0",
      "X-Amz-Target"  => "",
      "Authorization" => "AWS4-HMAC-SHA256 Credential=#{user}/20260917/#{region}/sqs/aws4_request, " \
                         "SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=deadbeef",
    }
  end

  def call(action : String, params = NamedTuple.new, user = "guest", path = "/") : HTTP::Client::Response
    hdrs = headers(user)
    hdrs["X-Amz-Target"] = "AmazonSQS.#{action}"
    HTTP::Client.post("#{base_url}#{path}", headers: hdrs, body: params.to_json)
  end

  # Calls the action and returns the parsed response, failing on any error
  def json(action : String, params = NamedTuple.new, user = "guest", path = "/",
           file = __FILE__, line = __LINE__) : JSON::Any
    response = call(action, params, user, path)
    unless response.status_code == 200
      fail "#{action} failed: #{response.status_code} #{response.body}", file, line
    end
    response.content_type.should eq "application/x-amz-json-1.0"
    JSON.parse(response.body)
  end

  def error(action : String, params = NamedTuple.new, user = "guest", path = "/") : {Int32, String, JSON::Any}
    response = call(action, params, user, path)
    body = JSON.parse(response.body)
    {response.status_code, body["__type"].as_s.lchop("com.amazonaws.sqs#"), body}
  end

  def create_queue(name : String, attributes = nil) : String
    params = attributes ? {QueueName: name, Attributes: attributes} : {QueueName: name}
    json("CreateQueue", params)["QueueUrl"].as_s
  end
end

def with_sqs_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    sqs = s.sqs_server
    addr = sqs.bind_tcp("127.0.0.1", 0)
    spawn(name: "sqs listen") { sqs.listen }
    Fiber.yield
    yield({SQSSpecClient.new(addr), s})
  end
end
