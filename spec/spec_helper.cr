require "spec"
require "file_utils"
require "../src/avalanchemq/server"
require "../src/avalanchemq/http/http_server"
require "http/client"
require "amqp"
require "uri"
#require "specreporter-spec"

FileUtils.rm_rf("/tmp/spec")

{% if flag?(:verbose) %}
  LOG_LEVEL = Logger::DEBUG
{% else %}
  LOG_LEVEL = Logger::ERROR
{% end %}

#Spec.override_default_formatter(
  # indent_string: "    ",        # Indent string. Default "  "
  # width: ENV["COLUMNS"].to_i-2, # Terminal width. Default 78
  # ^-- You may need to run "eval `resize`" in term to get COLUMNS variable
  # elapsed_width: 8,     # Number of decimals for "elapsed" time. Default 3
  # status_width: 10,     # Width of the status field. Default 5
  # trim_exceptions: false,     # Hide callstack from exceptions? Default true
  # skip_errors_report: false,  # Skip default backtraces. Default true
  # skip_slowest_report: false, # Skip default "slowest" report. Default true
  # skip_failed_report: false,  # Skip default failed reports summary. Default true
  #Spec::SpecReporterFormatter.new(width: 100)
#)

module TestHelpers
  def wait_for(t = 1.seconds)
    timeout = Time.now + t
    until yield
      Fiber.yield
      if Time.now > timeout
        puts "Execuction expired"
        return
      end
    end
  end

  def test_headers(headers = nil)
    req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                             "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
    req_hdrs.merge!(headers) if headers
    req_hdrs
  end

  def create_servers(dir = "/tmp/spec", level = LOG_LEVEL)
    s = AvalancheMQ::Server.new(dir, level)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    {s, h}
  end

  def amqp_server(dir = "/tmp/spec", level = LOG_LEVEL)
    AvalancheMQ::Server.new(dir, level)
  end

  def listen(server : (AvalancheMQ::HTTPServer | AvalancheMQ::Server), port : Int)
    spawn { server.listen(port) }
    Fiber.yield
  end

  def listen(*servers)
    servers.each do |s|
      spawn { s.try &.listen }
    end
    Fiber.yield
  end

  def close(*servers)
    servers.each do |s|
      spawn { s.try &.close }
    end
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
