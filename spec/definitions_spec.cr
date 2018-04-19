require "./spec_helper"
require "../src/avalanchemq/http_server"
require "http/client"

describe AvalancheMQ::HTTPServer do
  it "GET /" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.listen }
    Fiber.yield
    response = HTTP::Client.get "http://localhost:8080/"
    response.status_code.should eq 200
  end

  it "POST /api/definitions" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.listen }
    Fiber.yield
    s.users["bunny_gem"]?.should be_nil
    body = %({
      "rabbit_version":"3.7.4", "users":[{
        "name":"bunny_gem",
        "password_hash":"rynF7rEzCknjtWTiPKuobNIVNjy/0CpOnaAoI6XFImG4RBJd",
        "hashing_algorithm":"rabbit_password_hashing_sha256","tags":""
      }]
    })
    response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                 headers: HTTP::Headers{"Content-Type" => "application/json"},
                                 body: body)
    response.status_code.should eq 200
    s.users["bunny_gem"]?.should be_truthy
  end
end
