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
    h.close
    s.close
  end

  describe "POST /api/definitions" do
    it "accepts users" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.users.delete("bunny_gem")
      body = %({
        "users":[{
          "name":"bunny_gem",
          "password_hash":"rynF7rEzCknjtWTiPKuobNIVNjy/0CpOnaAoI6XFImG4RBJd",
          "hashing_algorithm":"rabbit_password_hashing_sha256","tags":""
        }]
      })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      user = s.users["bunny_gem"]? || nil
      user.should be_a(AvalancheMQ::User)
      ok = user.not_nil!.password == "bunny_password"
      ok.should be_true
      h.close
      s.close
    end

    it "accepts permissions" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.vhosts.delete("def")
      body = %({ "vhosts":[{ "name":"def" }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      vhost = s.vhosts["def"]? || nil
      vhost.should be_a(AvalancheMQ::VHost)
      h.close
      s.close
    end
  end
end
