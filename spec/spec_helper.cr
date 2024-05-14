require "spec"
require "file_utils"
require "../src/lavinmq/config"
require "http/client"
require "amqp-client"
require "string_scanner"

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

def with_channel(file = __FILE__, line = __LINE__, **args, &)
  name = "lavinmq-spec-#{file}:#{line}"
  args = {port: LavinMQ::Config.instance.amqp_port, name: name}.merge(args)
  conn = AMQP::Client.new(**args).connect
  ch = conn.channel
  yield ch, conn
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
  spawn { s.listen_replication("127.0.0.1", LavinMQ::Config.instance.replication_port) }
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

Server = start_amqp_server
start_http_server

Spec.after_each do
  Server.stop
  FileUtils.rm_rf("/tmp/lavinmq-spec")
end

Spec.before_each do
  Server.stop
  FileUtils.rm_rf("/tmp/lavinmq-spec")
  Server.restart
end


class Invalid < Exception
  def initialize
    super("invalid input")
  end
end

KEY_RE        = /[\w:]+/
VALUE_RE      = /-?\d+\.?\d*E?-?\d*|NaN/
ATTR_KEY_RE   = /[ \w-]+/
ATTR_VALUE_RE = %r{\s*"([\\"'\sa-zA-Z0-9\-_/.+]*)"\s*}

def parse_prometheus(raw)
  s = StringScanner.new(raw)
  res = [] of NamedTuple(key: String, attrs: Hash(String, String), value: Float64)
  until s.eos?
    if s.peek(1) == "#"
      s.scan(/.*\n/)
      next
    end
    key = s.scan KEY_RE
    raise Invalid.new unless key
    attrs = parse_attrs(s)
    value = s.scan VALUE_RE
    raise Invalid.new unless value
    value = value.to_f
    s.scan(/\n/)
    res.push({key: key, attrs: attrs, value: value})
  end
  res
end

def parse_attrs(s)
  attrs = Hash(String, String).new
  if s.scan(/\s|{/) == "{"
    loop do
      if s.peek(1) == "}"
        s.scan(/}/)
        break
      end
      key = s.scan ATTR_KEY_RE
      raise Invalid.new unless key
      key = key.strip
      s.scan(/=/)
      s.scan ATTR_VALUE_RE

      value = s[1]
      raise Invalid.new unless value
      attrs[key] = value
      break if s.scan(/,|}/) == "}"
    end
    s.scan(/\s/)
  end
  attrs
end
