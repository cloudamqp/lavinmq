require "./spec_helper"

describe AvalancheMQ::StaticController do
  it "GET /" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.listen }
    Fiber.yield
    response = HTTP::Client.get "http://localhost:8080/"
    response.status_code.should eq 200
    h.close
    s.close
  end
end
