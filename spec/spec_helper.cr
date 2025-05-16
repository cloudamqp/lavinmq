require "log"
Log.setup_from_env(default_level: :error)

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
require "http/client"
require "amqp-client"

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

def test_headers(headers = nil)
  req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                           "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
  req_hdrs.merge!(headers) if headers
  req_hdrs
end

def with_amqp_server(tls = false, replicator = LavinMQ::Clustering::NoopServer.new,
                     config = LavinMQ::Config.instance, & : LavinMQ::Server -> Nil)
  LavinMQ::Config.instance = init_config(config)
  FileUtils.rm_rf(config.data_dir)
  tcp_server = TCPServer.new("localhost", ENV.has_key?("NATIVE_PORTS") ? 5672 : 0)
  s = LavinMQ::Server.new(config, replicator)
  begin
    if tls
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"
      spawn(name: "amqp tls listen") { s.listen_tls(tcp_server, ctx, LavinMQ::Server::Protocol::AMQP) }
    else
      spawn(name: "amqp tcp listen") { s.listen(tcp_server, LavinMQ::Server::Protocol::AMQP) }
    end
    Fiber.yield
    yield s
  ensure
    s.close
    FileUtils.rm_rf(config.data_dir)
    LavinMQ::Config.instance = init_config(LavinMQ::Config.new)
  end
end

def with_http_server(&)
  with_amqp_server do |s|
    h = LavinMQ::HTTP::Server.new(s)
    begin
      addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15672 : 0)
      spawn(name: "http listen") { h.listen }
      Fiber.yield
      yield({HTTPSpecHelper.new(addr.to_s), s})
    ensure
      h.close
    end
  end
end

struct HTTPSpecHelper
  def initialize(@addr : String)
  end

  getter addr

  def get(path, headers = nil)
    HTTP::Client.get("http://#{@addr}#{path}", headers: test_headers(headers))
  end

  def post(path, headers = nil, body = nil)
    HTTP::Client.post("http://#{@addr}#{path}", headers: test_headers(headers), body: body)
  end

  def put(path, headers = nil, body = nil)
    HTTP::Client.put("http://#{@addr}#{path}", headers: test_headers(headers), body: body)
  end

  def delete(path, headers = nil, body = nil)
    HTTP::Client.delete("http://#{@addr}#{path}", headers: test_headers(headers), body: body)
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
