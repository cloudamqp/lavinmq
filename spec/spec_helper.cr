require "log"
Log.setup_from_env(default_level: :error)

Signal::SEGV.reset # Let the OS generate a coredump

class Log
  def self.setup
    # noop, don't override during spec
  end
end

require "spec"
require "file_utils"
require "../src/lavinmq/config" # have to be required first
require "../src/lavinmq/server"
require "../src/lavinmq/http/http_server"
require "../src/lavinmq/http/metrics_server"
require "../src/stdlib/openssl_x509_store"
require "http/client"
require "amqp-client"
require "./support/*"

def init_config(config = LavinMQ::Config.instance)
  config.data_dir = "/tmp/lavinmq-spec"
  config.segment_size = 512 * 1024
  config.consumer_timeout_loop_interval = 1
  config
end

init_config

# Allow creating custom config objects for specs
module LavinMQ
  class Config
    def initialize
    end

    def self.instance=(instance)
      @@instance = instance
    end
  end
end

def with_datadir(&)
  data_dir = File.tempname("lavinmq", "spec")
  Dir.mkdir_p data_dir
  yield data_dir
ensure
  FileUtils.rm_rf data_dir if data_dir
end

def with_channel(s : LavinMQ::Server, file = __FILE__, line = __LINE__, **args, &)
  name = "lavinmq-spec-#{file}:#{line}"
  s.@listeners
    .select { |k, v| k.is_a?(TCPServer) && v.amqp? }
    .keys
    .select(TCPServer)
    .first
    .local_address
    .port
  args = {port: amqp_port(s), name: name}.merge(args)
  conn = AMQP::Client.new(**args).connect
  ch = conn.channel
  yield ch
ensure
  conn.try &.close(no_wait: false)
end

def amqp_port(s)
  s.@listeners.keys.select(TCPServer).first.local_address.port
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

def with_amqp_server(tls = false, replicator = nil,
                     config = LavinMQ::Config.instance,
                     file = __FILE__, line = __LINE__, & : LavinMQ::Server -> Nil)
  LavinMQ::Config.instance = init_config(config)

  # If TLS is requested but config doesn't have TLS configured, set up default TLS
  if tls && !config.tls_configured?
    config.tls_cert_path = "spec/resources/server_certificate.pem"
    config.tls_key_path = "spec/resources/server_key.pem"
  end

  tcp_server = TCPServer.new("localhost", ENV.has_key?("NATIVE_PORTS") ? 5672 : 0)
  s = LavinMQ::Server.new(config, replicator)
  begin
    if tls
      spawn(name: "amqp tls listen") { s.listen_tls(tcp_server, LavinMQ::Server::Protocol::AMQP) }
    else
      spawn(name: "amqp tcp listen") { s.listen(tcp_server, LavinMQ::Server::Protocol::AMQP) }
    end
    Fiber.yield
    yield s
  ensure
    # A closed queue indicates that something failed that shouldn't. However, the error may be
    # silent and not causing a spec to fail. It's not even sure that the failing queue is related
    # to what the spec tests, but by failing on closed queues we may find bugs.
    # If a queue is supposed to be closed, it should be deleted in the end of the spec.

    # We yield a few times (if necessary) to make sure things has really settled, e.g.
    # everything has been cleaned up after a `with_channel` inside the `with_amqp_server`.
    closed_queues = 3.times do
      Fiber.yield
      queues = s.vhosts.flat_map { |_, vhost| vhost.queues.values.select &.closed? }
      break if queues.empty?
      queues
    end
    if closed_queues && !closed_queues.empty?
      msg = "Closed queues detected: #{closed_queues.map(&.name).join(", ")}\n\n" \
            "If they should be closed, please delete them in the end of the spec."
      raise Spec::AssertionFailed.new(msg, file, line)
    end
    s.close
    FileUtils.rm_rf(config.data_dir)
    LavinMQ::Config.instance = init_config(LavinMQ::Config.new)
  end
end

# Helper for MQTTS with mTLS support
def with_mqtts_server(verify_peer = false, require_peer_cert = false, replicator = nil,
                      config = LavinMQ::Config.instance,
                      file = __FILE__, line = __LINE__,
                      & : LavinMQ::Server, Int32 -> Nil)
  LavinMQ::Config.instance = init_config(config)
  FileUtils.rm_rf(config.data_dir)

  # Configure TLS via config
  config.tls_cert_path = "spec/resources/server_certificate.pem"
  config.tls_key_path = "spec/resources/server_key.pem"
  if verify_peer
    config.tls_verify_peer = true
    config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
    config.tls_fail_if_no_peer_cert = require_peer_cert
  end

  tcp_server = TCPServer.new("localhost", ENV.has_key?("NATIVE_PORTS") ? 8883 : 0)
  s = LavinMQ::Server.new(config, replicator)
  begin
    spawn(name: "mqtts tls listen") { s.listen_tls(tcp_server, LavinMQ::Server::Protocol::MQTT) }
    port = tcp_server.local_address.port
    # Wait for the server to actually start listening
    10.times do
      Fiber.yield
      sleep 5.milliseconds
    end
    yield s, port
  ensure
    s.close
    FileUtils.rm_rf(config.data_dir)
    LavinMQ::Config.instance = init_config(LavinMQ::Config.new)
  end
end

def with_http_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    h = LavinMQ::HTTP::Server.new(s)
    begin
      addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15672 : 0)
      spawn(name: "http listen") { h.listen }
      Fiber.yield
      yield({HTTPSpecHelper.new(addr), s})
    ensure
      h.close
    end
  end
end

def with_metrics_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    h = LavinMQ::HTTP::MetricsServer.new(s)
    begin
      addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15692 : 0)
      spawn(name: "http listen") { h.listen }
      Fiber.yield
      yield({HTTPSpecHelper.new(addr), s})
    ensure
      h.close
    end
  end
end

struct HTTPSpecHelper
  def initialize(@addr : Socket::IPAddress)
  end

  getter addr

  def test_headers(headers = nil)
    req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                             "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
    req_hdrs.merge!(headers) if headers
    req_hdrs
  end

  def test_uri(path)
    "http://#{@addr}#{path}"
  end

  def get(path, headers = nil)
    HTTP::Client.get(test_uri(path), headers: test_headers(headers))
  end

  def post(path, headers = nil, body = nil)
    HTTP::Client.post(test_uri(path), headers: test_headers(headers), body: body)
  end

  def put(path, headers = nil, body = nil)
    HTTP::Client.put(test_uri(path), headers: test_headers(headers), body: body)
  end

  def delete(path, headers = nil, body = nil)
    HTTP::Client.delete(test_uri(path), headers: test_headers(headers), body: body)
  end

  def exec(request : HTTP::Request)
    HTTP::Client.new(URI.parse "http://#{@addr}").exec(request)
  end
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

def exit(code = 0)
  raise SpecExit.new(code)
end

class SpecExit < Exception
  getter code : Int32

  def initialize(@code)
    super "Exiting with code #{@code}"
  end
end

module LavinMQ
  # Allow creating new Config object without using the singleton
  class Config
    def initialize
    end
  end
end
