require "spec"
require "file_utils"
require "../src/lavinmq/config"
require "http/client"
require "amqp-client"

Log.setup_from_env

Spec.override_default_formatter(Spec::VerboseFormatter.new)

AMQP_PORT      = ENV.fetch("AMQP_PORT", "5672").to_i
AMQPS_PORT     = ENV.fetch("AMQPS_PORT", "5671").to_i
AMQP_BASE_URL  = "amqp://localhost:#{AMQP_PORT}"
AMQPS_BASE_URL = "amqps://localhost:#{AMQPS_PORT}"
HTTP_PORT      = ENV.fetch("HTTP_PORT", "8080").to_i
BASE_URL       = "http://localhost:#{HTTP_PORT}"
DATA_DIR       = "/tmp/lavinmq-spec"

Dir.mkdir_p DATA_DIR
LavinMQ::Config.instance.tap do |cfg|
  cfg.data_dir = DATA_DIR
  cfg.amqp_port = AMQP_PORT
  cfg.amqps_port = AMQPS_PORT
  cfg.http_port = HTTP_PORT
  cfg.segment_size = 512 * 1024
end

# have to be required after config
require "../src/lavinmq/server"
require "../src/lavinmq/http/http_server"

def with_channel(**args, &)
  name = nil
  if formatter = Spec.formatters[0].as?(Spec::VerboseFormatter)
    name = formatter.@last_description
  end
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
  spawn { s.listen(LavinMQ::Config.instance.amqp_bind, LavinMQ::Config.instance.amqp_port) }
  spawn { s.listen_replication("127.0.0.1", LavinMQ::Config.instance.replication_port) }
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  spawn { s.listen_tls(LavinMQ::Config.instance.amqp_bind, LavinMQ::Config.instance.amqps_port, ctx) }
  s
end

def start_http_server
  h = LavinMQ::HTTP::Server.new(Server)
  h.bind_tcp(LavinMQ::Config.instance.http_bind, LavinMQ::Config.instance.http_port)
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
  HTTP::Client.get("#{BASE_URL}#{path}", headers: test_headers(headers))
end

def post(path, headers = nil, body = nil)
  HTTP::Client.post("#{BASE_URL}#{path}", headers: test_headers(headers), body: body)
end

def put(path, headers = nil, body = nil)
  HTTP::Client.put("#{BASE_URL}#{path}", headers: test_headers(headers), body: body)
end

def delete(path, headers = nil, body = nil)
  HTTP::Client.delete("#{BASE_URL}#{path}", headers: test_headers(headers), body: body)
end

# If we can connect to the port, it's busy by another listener
def port_busy?(port)
  sock = Socket.tcp(Socket::Family::INET)
  begin
    sock.connect "localhost", port
    true
  rescue Socket::ConnectError
    false
  ensure
    sock.close
  end
end

{AMQP_PORT, AMQPS_PORT, HTTP_PORT}.each do |port|
  if port_busy?(port)
    puts "TCP port #{port} collision!"
    puts "Make sure no other process is listening to port #{port}"
    Spec.abort!
  end
end

Server = start_amqp_server
start_http_server

Spec.after_each do
  Server.stop
  FileUtils.rm_rf("/tmp/lavinmq-spec")
  Server.restart
end
