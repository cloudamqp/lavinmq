require "../spec_helper"

describe AvalancheMQ::ExchangesController do
  describe "GET /api/exchanges" do
    it "should return all exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["arguments", "internal", "auto_delete", "durable", "type", "vhost", "name"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost" do
    it "should return all exchanges for a vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost/name" do
    it "should return exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/amq.topic",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should return 404 if exchange does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/404",
                                  headers: test_headers)
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/exchanges/vhost/name" do
    it "should create exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic",
        "durable": false,
        "internal": false,
        "auto_delete": true,
        "arguments": {
          "alternate-exchange": "spexchange"
        }
      })
      s.vhosts["/"].delete_exchange("spechange")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should require type" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({})
      s.vhosts["/"].delete_exchange("faulty")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/faulty",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should require durable to be the same when overwriting" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic",
        "durable": true,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      s.vhosts["/"].delete_exchange("spechange")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      body = %({
        "type": "topic",
        "durable": false,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should not be possible to declare amq. prefixed exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic"
      })
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/amq.test",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should require config access to declare" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.create("test_perm", "pw")
      s.users.add_permission("test_perm", "/", /^$/, /^$/, /^$/)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic"
      })
      hdrs = HTTP::Headers{"Content-Type" => "application/json",
                           "Authorization" => "Basic dGVzdF9wZXJtOnB3"}
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/test_perm",
                                  headers: hdrs,
                                  body: body)
      response.status_code.should eq 401
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/exchanges/vhost/name" do
    it "should delete exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      response = HTTP::Client.delete("http://localhost:8080/api/exchanges/%2f/spechange",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end
end
