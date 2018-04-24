require "./spec_helper"
require "../src/avalanchemq/http/http_server"
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
    it "imports users" do
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

    it "imports vhosts" do
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

    it "imports queues" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.vhosts["/"].delete_queue("q1")
      body = %({ "queues": [{ "name": "q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].queues.has_key?("q1").should be_true
      h.close
      s.close
    end

    it "imports exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.vhosts["/"].delete_exchange("x1")
      body = %({ "exchanges": [{ "name": "x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges.has_key?("x1").should be_true
      h.close
      s.close
    end

    it "imports bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("x1", "direct", false, true)
      s.vhosts["/"].declare_exchange("x2", "fanout", false, true)
      s.vhosts["/"].declare_queue("q1", false, true)
      body = %({ "bindings": [
        {
          "source": "x1",
          "vhost": "/",
          "destination": "x2",
          "destination_type": "exchange",
          "routing_key": "r.k2",
          "arguments": {}
        },
        {
          "source": "x1",
          "vhost": "/",
          "destination": "q1",
          "destination_type": "queue",
          "routing_key": "rk",
          "arguments": {}
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges["x1"].matches("r.k2", nil).map(&.name).includes?("x2").should be_true
      s.vhosts["/"].exchanges["x1"].matches("rk", nil).map(&.name).includes?("q1").should be_true
      s.vhosts["/"].delete_queue("q1")
      s.vhosts["/"].delete_exchange("x1")
      s.vhosts["/"].delete_exchange("x2")
      h.close
      s.close
    end

    it "imports permissions" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.listen }
      Fiber.yield
      s.users.create("u1", "")
      body = %({ "permissions": [
        {
          "user": "u1",
          "vhost": "/",
          "configure": "c",
          "write": "w",
          "read": "r"
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.users["u1"].permissions["/"][:write].should eq(/w/)
      h.close
      s.close
    end
  end
end
