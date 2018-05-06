require "spec"
require "file_utils"
require "../src/avalanchemq/server"
require "../src/avalanchemq/http/http_server"
require "http/client"
require "amqp"
require "uri"

FileUtils.rm_rf("/tmp/spec")

module TestHelpers
  def wait_for
    timeout = Time.now + 1.seconds
    until yield
      Fiber.yield
      if Time.now > timeout
        puts "Execuction expired"
        return
      end
    end
  end

  def test_headers(headers = nil)
    req_hdrs = HTTP::Headers{"Content-Type" => "application/json",
                             "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
    req_hdrs.merge!(headers) if headers
    req_hdrs
  end

  def create_servers(dir = "/tmp/spec", level = Logger::ERROR)
    s = AvalancheMQ::Server.new(dir, level)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    { s, h }
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
