require "spec"
require "file_utils"
require "../src/lavinmq/config"
require "http/client"
require "amqp-client"

Log.setup_from_env

DATA_DIR = "/tmp/lavinmq-spec"

Dir.mkdir_p DATA_DIR
LavinMQ::Config.instance.tap do |cfg|
  cfg.data_dir = DATA_DIR
  cfg.segment_size = 512 * 1024
  cfg.consumer_timeout_loop_interval = 1
end

module SpecHelper
  def self.amqp_base_url
    "amqp://localhost:#{LavinMQ::Config.instance.amqp_port}"
  end

  def self.amqps_base_url
    "amqps://localhost:#{LavinMQ::Config.instance.amqps_port}"
  end

  def self.http_base_url
    "http://localhost:#{LavinMQ::Config.instance.http_port}"
  end

  def self.ws_base_url
    "ws://localhost:#{LavinMQ::Config.instance.http_port}"
  end
end

# have to be required after config
require "../src/lavinmq/server"
require "../src/lavinmq/http/http_server"

def with_datadir(&)
  data_dir = File.tempname("lavinmq", "spec")
  Dir.mkdir_p data_dir
  yield data_dir
ensure
  FileUtils.rm_rf data_dir if data_dir
end

def with_channel(file = __FILE__, line = __LINE__, **args, &)
  name = "lavinmq-spec-#{file}:#{line}"
  args = {port: LavinMQ::Config.instance.amqp_port, name: name}.merge(args)
  conn = AMQP::Client.new(**args).connect
  ch = conn.channel
  yield ch
ensure
  conn.try &.close(no_wait: false)
end

def with_vhost(name, &)
  vhost = Server.vhosts.create(name)
  yield vhost
ensure
  Server.vhosts.delete(name)
end

def with_ssl_channel(**args, &)
  with_channel(args) { |ch| yield ch }
end

def should_eventually(expectation, timeout = 5.seconds, file = __FILE__, line = __LINE__, &)
  sec = Time.monotonic
  loop do
    Fiber.yield
    begin
      yield.should(expectation, file: file, line: line)
      return
    rescue ex
      raise ex if Time.monotonic - sec > timeout
    end
  end
end

def wait_for(timeout = 5.seconds, file = __FILE__, line = __LINE__, &)
  sec = Time.monotonic
  loop do
    Fiber.yield
    res = yield
    return res if res
    break if Time.monotonic - sec > timeout
  end
  fail "Execution expired", file: file, line: line
end

def test_headers(headers = nil)
  req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                           "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
  req_hdrs.merge!(headers) if headers
  req_hdrs
end

def start_amqp_server
  s = LavinMQ::Server.new(DATA_DIR)
  tcp_server = TCPServer.new(LavinMQ::Config.instance.amqp_bind, 0)
  LavinMQ::Config.instance.amqp_port = tcp_server.local_address.port
  spawn { s.listen(tcp_server) }
  spawn { s.listen_clustering("127.0.0.1", LavinMQ::Config.instance.clustering_port) }
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  tls_server = TCPServer.new(LavinMQ::Config.instance.amqp_bind, 0)
  LavinMQ::Config.instance.amqps_port = tls_server.local_address.port
  spawn { s.listen_tls(tls_server, ctx) }
  s
end

def start_http_server
  h = LavinMQ::HTTP::Server.new(Server)
  addr = h.bind_tcp(LavinMQ::Config.instance.http_bind, 0)
  LavinMQ::Config.instance.http_port = addr.port
  spawn { h.listen }
  Fiber.yield
  h
end

# Helper function for creating a queue with ttl and an associated dlq
def create_ttl_and_dl_queues(channel, queue_ttl = 1)
  args = AMQP::Client::Arguments.new
  args["x-message-ttl"] = queue_ttl
  args["x-dead-letter-exchange"] = ""
  args["x-dead-letter-routing-key"] = "dlq"
  q = channel.queue("ttl", args: args)
  dlq = channel.queue("dlq")
  {q, dlq}
end

def get(path, headers = nil)
  HTTP::Client.get("#{SpecHelper.http_base_url}#{path}", headers: test_headers(headers))
end

def post(path, headers = nil, body = nil)
  HTTP::Client.post("#{SpecHelper.http_base_url}#{path}", headers: test_headers(headers), body: body)
end

def put(path, headers = nil, body = nil)
  HTTP::Client.put("#{SpecHelper.http_base_url}#{path}", headers: test_headers(headers), body: body)
end

def delete(path, headers = nil, body = nil)
  HTTP::Client.delete("#{SpecHelper.http_base_url}#{path}", headers: test_headers(headers), body: body)
end

Server = start_amqp_server
start_http_server

Spec.after_each do
  Server.stop
  FileUtils.rm_rf("/tmp/lavinmq-spec")
  Server.restart
end
