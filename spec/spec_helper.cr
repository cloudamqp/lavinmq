require "spec"
require "file_utils"
require "amqp-client"
require "../src/lavinmq/config"
require "../src/lavinmq/server"
require "../src/lavinmq/log_formatter"
require "../src/lavinmq/http/http_server"
require "http/client"
require "socket"
require "uri"

Log.setup_from_env

Spec.override_default_formatter(Spec::VerboseFormatter.new)

AMQP_PORT  = ENV.fetch("AMQP_PORT", "0").to_i
AMQPS_PORT = ENV.fetch("AMQPS_PORT", "0").to_i
HTTP_PORT  = ENV.fetch("HTTP_PORT", "0").to_i
DATA_DIR   = "/tmp/lavinmq-spec"

LavinMQ::Config.instance.tap do |cfg|
  cfg.gc_segments_interval = 1
  cfg.queue_max_acks = 10
  cfg.segment_size = 512 * 1024
  cfg.amqp_port = AMQP_PORT
  cfg.amqps_port = AMQPS_PORT
  cfg.http_port = HTTP_PORT
end

module Helper
  extend self

  def amqp_base_url
    "amqp://localhost:#{LavinMQ::Config.instance.amqp_port}"
  end

  def amqps_base_url
    "amqps://localhost:#{LavinMQ::Config.instance.amqps_port}"
  end

  def base_url
    "http://localhost:#{LavinMQ::Config.instance.http_port}"
  end

  def http_port
    LavinMQ::Config.instance.http_port
  end

  def amqp_port
    LavinMQ::Config.instance.amqp_port
  end

  def amqps_port
    LavinMQ::Config.instance.amqps_port
  end
end

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
  amqp_tcp_server = TCPServer.new(port: AMQP_PORT)
  s = LavinMQ::Server.new(DATA_DIR)
  # spawn { s.listen(LavinMQ::Config.instance.amqp_bind, LavinMQ::Config.instance.amqp_port) }
  spawn { s.listen(amqp_tcp_server) }
  cert = Dir.current + "/spec/resources/server_certificate.pem"
  key = Dir.current + "/spec/resources/server_key.pem"
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = cert
  ctx.private_key = key
  amqps_tcp_server = TCPServer.new(port: AMQPS_PORT)
  spawn { s.listen_tls(amqps_tcp_server, ctx) }

  LavinMQ::Config.instance.tap do |cfg|
    cfg.amqp_port = amqp_tcp_server.local_address.port
    cfg.amqps_port = amqps_tcp_server.local_address.port
  end

  s
end

def start_http_server
  h = LavinMQ::HTTP::Server.new(Server)
  addr = h.bind_tcp(LavinMQ::Config.instance.http_bind, HTTP_PORT)
  LavinMQ::Config.instance.tap do |cfg|
    cfg.http_port = addr.port
  end
  spawn { h.listen }
  Fiber.yield
  h
end

def get(path, headers = nil)
  HTTP::Client.get("#{Helper.base_url}#{path}", headers: test_headers(headers))
end

def post(path, headers = nil, body = nil)
  HTTP::Client.post("#{Helper.base_url}#{path}", headers: test_headers(headers), body: body)
end

def put(path, headers = nil, body = nil)
  HTTP::Client.put("#{Helper.base_url}#{path}", headers: test_headers(headers), body: body)
end

def delete(path, headers = nil, body = nil)
  HTTP::Client.delete("#{Helper.base_url}#{path}", headers: test_headers(headers), body: body)
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
  next if port == 0
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
