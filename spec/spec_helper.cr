require "spec"
require "file_utils"
require "../src/avalanchemq/server"
require "../src/avalanchemq/http/http_server"
require "http/client"
require "amqp"
require "uri"

FileUtils.rm_rf("/tmp/spec")

{% if flag?(:verbose) %}
  LOG_LEVEL = Logger::DEBUG
{% elsif flag?(:warn) %}
  LOG_LEVEL = Logger::WARN
{% else %}
  LOG_LEVEL = Logger::ERROR
{% end %}

Spec.override_default_formatter(Spec::VerboseFormatter.new)

module TestHelpers
  class_property s, h

  def self.setup
    create_servers
    spawn { @@s.try &.listen(5672) }
    cert = Dir.current + "/spec/resources/server_certificate.pem"
    key = Dir.current + "/spec/resources/server_key.pem"
    ca = Dir.current + "/spec/resources/ca_certificate.pem"
    spawn { @@s.try &.listen_tls(5671, cert, key, ca) }
    spawn { @@h.try &.listen }
    Fiber.yield
  end

  def s
    TestHelpers.s.not_nil!
  end

  def h
    TestHelpers.h.not_nil!
  end

  def wait_for(t = 10.seconds)
    timeout = Time.now + t
    until yield
      Fiber.yield
      if Time.now > timeout
        raise "Execuction expired"
      end
    end
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
    @@s = AvalancheMQ::Server.new(dir, level)
    @@h = AvalancheMQ::HTTPServer.new(@@s.not_nil!, 8080)
  end

  def get(url, headers = nil)
    HTTP::Client.get(url, headers: test_headers(headers))
  end

  def post(url, headers = nil, body = nil)
    HTTP::Client.post(url, headers: test_headers(headers), body: body)
  end

  def put(url, headers = nil, body = nil)
    HTTP::Client.put(url, headers: test_headers(headers), body: body)
  end

  def delete(url, headers = nil, body = nil)
    HTTP::Client.delete(url, headers: test_headers(headers), body: body)
  end
end

extend TestHelpers
TestHelpers.setup
