require "../spec_helper"

describe AvalancheMQ::BindingsController do
  describe "GET /api/bindings" do
    it "should return all bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments",
              "properties_key"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/bindings/vhost" do
    it "should return all bindings for a vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue" do
    it "should return bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/q/q1",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end

    it "should return 404 if exchange does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/404/q/404",
                                  headers: test_headers)
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "POST /api/bindings/vhost/e/exchange/q/queue" do
    it "should create binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = HTTP::Client.post("http://localhost:8080/api/bindings/%2f/e/be1/q/q1",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 200
      response.headers["Location"].should match /api\/bindings\/%2f\/e\/be1\/q\/q1\/.*/
    ensure
      h.try &.close
    end

    it "should return 404 if exchange does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/404/q/404",
                                  headers: test_headers)
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should return binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/q/q1",
                                  headers: test_headers)
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/q/q1/#{props}",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should delete binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/q/q1",
                                  headers: test_headers)
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = HTTP::Client.delete("http://localhost:8080/api/bindings/%2f/e/be1/q/q1/#{props}",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination" do
    it "should return bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "POST /api/bindings/vhost/e/source/e/destination" do
    it "should create binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = HTTP::Client.post("http://localhost:8080/api/bindings/%2f/e/be1/e/be2",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination/props" do
    it "should return binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2",
                                  headers: test_headers)
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2/#{props}",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/bindings/vhost/e/source/e/destination/props" do
    it "should delete binding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2",
                                  headers: test_headers)
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = HTTP::Client.delete("http://localhost:8080/api/bindings/%2f/e/be1/e/be2/#{props}",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end
end
