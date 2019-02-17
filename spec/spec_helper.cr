require "spec"
require "file_utils"
require "amqp-client"
require "../src/avalanchemq/config"
require "../src/avalanchemq/server"
require "../src/avalanchemq/log_formatter"
require "../src/avalanchemq/http/http_server"
require "http/client"
require "uri"

FileUtils.rm_rf("/tmp/spec")

{% if flag?(:verbose) %}
  LOG_LEVEL = Logger::DEBUG
{% elsif flag?(:warn) %}
  LOG_LEVEL = Logger::WARN
{% else %}
  LOG_LEVEL = Logger::ERROR
{% end %}

AMQP_PORT      = ENV.fetch("AMQP_PORT", "5672").to_i
AMQPS_PORT     = ENV.fetch("AMQPS_PORT", "5671").to_i
AMQP_BASE_URL  = "amqp://localhost:#{AMQP_PORT}"
AMQPS_BASE_URL = "amqps://localhost:#{AMQPS_PORT}"
HTTP_PORT      = ENV.fetch("HTTP_PORT", "8080").to_i
BASE_URL       = "http://localhost:#{HTTP_PORT}"

Spec.override_default_formatter(Spec::VerboseFormatter.new)

module TestHelpers
  class_property s, h

  def self.setup
    create_servers
  end

  def s
    TestHelpers.s.not_nil!
  end

  def h
    TestHelpers.h.not_nil!
  end

  def with_channel(**args)
    AMQP::Client.start(**args.merge(port: AMQP_PORT)) do |conn|
      ch = conn.channel
      yield ch
    end
  end

  def with_ssl_channel(**args)
    with_channel(args) { |ch| yield ch }
  end

  def wait_for(timeout = 5.seconds)
    s = Time.monotonic
    until res = yield
      Fiber.yield
      raise "Execuction expired" if Time.monotonic - s > timeout
    end
    res
  rescue e
    puts "\n#{e.inspect_with_backtrace}"
  end

  def test_headers(headers = nil)
    req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                             "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
    req_hdrs.merge!(headers) if headers
    req_hdrs
  end

  def close_servers
    s.close
    h.close
  end

  def self.create_servers(dir = "/tmp/spec", level = LOG_LEVEL)
    log = Logger.new(STDOUT, level: level)
    AvalancheMQ::LogFormatter.use(log)
    @@s = AvalancheMQ::Server.new(dir, log.dup)
    @@h = AvalancheMQ::HTTP::Server.new(@@s.not_nil!, HTTP_PORT, log.dup)
    Spec.after_each do
      @@h.try &.cache.purge
    end
    spawn { @@s.try &.listen(AMQP_PORT) }
    cert = Dir.current + "/spec/resources/server_certificate.pem"
    key = Dir.current + "/spec/resources/server_key.pem"
    ca = Dir.current + "/spec/resources/ca_certificate.pem"
    spawn { @@s.try &.listen_tls(AMQPS_PORT, cert, key, ca) }
    spawn { @@h.try &.listen }
    Fiber.yield
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
end

extend TestHelpers
TestHelpers.setup
