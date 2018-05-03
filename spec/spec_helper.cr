require "spec"
require "file_utils"
require "../src/avalanchemq/server"
require "../src/avalanchemq/http/http_server"
require "http/client"
require "amqp"
require "uri"

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

  def test_headers
    HTTP::Headers{"Content-Type" => "application/json",
                  "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
  end
end
extend TestHelpers
